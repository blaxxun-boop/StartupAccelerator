using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using BepInEx;
using BepInEx.Bootstrap;
using BepInEx.Configuration;
using BepInEx.Logging;
using BepInEx.Preloader;
using HarmonyLib;
using HarmonyLib.Public.Patching;
using Mono.Cecil;
using Mono.Cecil.Cil;
using OpCodes = System.Reflection.Emit.OpCodes;

namespace StartupAccelerator;

public static class StartupAccelerator
{
	public static IEnumerable<string> TargetDLLs { get; } = Array.Empty<string>();
	public static void Patch(AssemblyDefinition assembly) { }

	private const string CONFIG_FILE_NAME = "StartupAccelerator.cfg";
	private static readonly ConfigFile Config = new(Path.Combine(Paths.ConfigPath, CONFIG_FILE_NAME), true);

	private static readonly ManualLogSource logger = Logger.CreateLogSource(nameof(StartupAccelerator));

	private static readonly Harmony delayedPatcherHarmony = new("org.bepinex.patchers.startupaccelerator.delayed_patcher");
	private static readonly Harmony harmony = new("org.bepinex.patchers.startupaccelerator");
	private static readonly MethodInfo harmonyPatcher = AccessTools.DeclaredMethod(typeof(Harmony).Assembly.GetType("HarmonyLib.PatchFunctions"), "UpdateWrapper");

	private static readonly string[] hardcodedPassthrough =
	{
		typeof(Assembly).FullName,
		"BepInEx.Preloader.RuntimeFixes.HarmonyInteropFix",
		"BepInEx.PluginInfo",
	};

	private static HashSet<string> passthroughClasses = new();

	private static readonly ConfigEntry<Toggle> delayedPatcher = Config.Bind("General", "Delay Patching", Toggle.On, new ConfigDescription("Delay Harmony patching until after Chainloader and after FejdStartup.Awake respectively."));
	private static readonly ConfigEntry<Toggle> unifiedLocalization = Config.Bind("General", "Merge Localization Data", Toggle.On, new ConfigDescription("Merge localization data to avoid re-reading it over and over."));
	private static readonly ConfigEntry<Toggle> optimizeConfigSave = Config.Bind("General", "Delay Config Save", Toggle.On, new ConfigDescription("Delay config save so that it saves the config once after start up and not over and over again."));

	private enum Toggle
	{
		On,
		Off,
	}

	private static readonly List<ConfigFile> changedConfigFiles = new();

	public static void Initialize()
	{
		ConfigEntry<string> passthrough = Config.Bind("General", "Passthrough Patched Classes", "", new ConfigDescription("Comma-separated list of classes to unconditionally patch immediately."));
		void calcPassthrough() => passthroughClasses = new HashSet<string>(hardcodedPassthrough.Concat(unifiedLocalization.Value == Toggle.On ? new[] { "Localization" } : Array.Empty<string>()).Concat(passthrough.Value.Split(',')));

		passthrough.SettingChanged += (_, _) => calcPassthrough();
		calcPassthrough();

		harmony.PatchAll(typeof(Patch_Preloader_PatchEntrypoint));
	}

	[HarmonyPatch]
	private static class Patch_Preloader_PatchEntrypoint
	{
		private static IEnumerable<MethodInfo> TargetMethods() => new[] { AccessTools.DeclaredMethod(typeof(EnvVars).Assembly.GetType("BepInEx.Preloader.Preloader"), "PatchEntrypoint") };

		private static void AddChainloaderFinishedCall(ILProcessor ilProcessor, Instruction instruction, AssemblyDefinition assembly) =>
			ilProcessor.InsertBefore(instruction, ilProcessor.Create(Mono.Cecil.Cil.OpCodes.Call, assembly.MainModule.ImportReference(ChainloaderStart)));

		private static readonly MethodInfo ChainloaderFinishedCallInstructionAdder = AccessTools.DeclaredMethod(typeof(Patch_Preloader_PatchEntrypoint), nameof(AddChainloaderFinishedCall));
		private static readonly MethodInfo ChainloaderStart = AccessTools.DeclaredMethod(typeof(Patch_Preloader_PatchEntrypoint), nameof(PreChainloader));
		private static readonly MethodInfo ILInstructionInserter = AccessTools.DeclaredMethod(typeof(ILProcessor), nameof(ILProcessor.InsertBefore));

		private static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
		{
			bool foundInitMethod = false;
			foreach (CodeInstruction instruction in instructions)
			{
				yield return instruction;
				if (instruction.opcode == OpCodes.Ldloc_S && instruction.operand is LocalBuilder { LocalIndex: 6 }) // initMethod
				{
					foundInitMethod = true;
				}
				else if (foundInitMethod && instruction.Calls(ILInstructionInserter))
				{
					yield return new CodeInstruction(OpCodes.Ldloc_S, 11); // ilProcessor
					yield return new CodeInstruction(OpCodes.Ldloc_S, 12); // target
					yield return new CodeInstruction(OpCodes.Ldarg_0); // assembly
					yield return new CodeInstruction(OpCodes.Ldind_Ref);
					yield return new CodeInstruction(OpCodes.Call, ChainloaderFinishedCallInstructionAdder);
				}
			}
		}

		private static bool patched = false;

		private static void PreChainloader()
		{
			if (!patched)
			{
				patched = true;

				if (delayedPatcher.Value == Toggle.On)
				{
					delayedPatcherHarmony.PatchAll(typeof(InterceptChainloader));
				}

				if (unifiedLocalization.Value == Toggle.On)
				{
					harmony.PatchAll(typeof(InterceptLocalization));
					harmony.PatchAll(typeof(InterceptLanguageLoad));
				}

				if (optimizeConfigSave.Value == Toggle.On)
				{
					harmony.PatchAll(typeof(DelayConfigSave));
					harmony.PatchAll(typeof(ChangeConfigSaveBack));
				}
			}
		}
	}

	[HarmonyPatch]
	private static class InterceptLocalization
	{
		private static readonly Dictionary<string, Dictionary<string, string>> localizationCache = new();
		private static readonly HashSet<MethodBase> alreadyAppliedEnglishLoadCSV = new();

		private static MethodInfo TargetMethod() => AccessTools.DeclaredMethod(Type.GetType("Localization, assembly_guiutils"), "SetupLanguage");

		[HarmonyPriority(Priority.Last)]
		private static bool Prefix(object __instance, string language, ref Dictionary<string, string> ___m_translations, ref bool __result)
		{
			if (localizationCache.TryGetValue(language, out Dictionary<string, string> translations))
			{
				___m_translations = translations;
				if (language == "English" && (string)AccessTools.DeclaredMethod(Type.GetType("UnityEngine.PlayerPrefs, UnityEngine.CoreModule")!, "GetString", new[] { typeof(string), typeof(string) }).Invoke(null, new[] { "language", "English" }) != "English")
				{
					foreach (Patch patch in AccessTools.DeclaredMethod(Type.GetType("Localization, assembly_guiutils"), "LoadCSV").ToPatchInfo().postfixes)
					{
						if (alreadyAppliedEnglishLoadCSV.Contains(patch.PatchMethod))
						{
							continue;
						}

						ParameterInfo[] parameterInfos = patch.PatchMethod.GetParameters();
						object[] parameters = new object[parameterInfos.Length];
						int index = 0;
						foreach (ParameterInfo parameter in parameterInfos)
						{
							if (parameter.Name == "__instance")
							{
								parameters[index] = __instance;
							}
							else if (parameter.Name == "language")
							{
								parameters[index] = language;
							}
							else if (parameter.Name == "__result")
							{
								parameters[index] = true;
							}
							else if (parameter.Name.StartsWith("___"))
							{
								parameters[index] = AccessTools.Field(__instance.GetType(), parameter.Name.Substring(3)).GetValue(__instance);
							}

							++index;
						}

						patch.PatchMethod.Invoke(null, parameters);
						alreadyAppliedEnglishLoadCSV.Add(patch.PatchMethod);
					}
				}

				__result = true;
				return false;
			}

			if (language != "English")
			{
				// Duplicate to avoid polluting the original English translation
				___m_translations = new Dictionary<string, string>(___m_translations);
			}
			return true;
		}

		[HarmonyPriority(Priority.Last)]
		private static void Postfix(string language, Dictionary<string, string> ___m_translations, bool __result)
		{
			if (__result)
			{
				localizationCache[language] = ___m_translations;
			}
		}
	}

	[HarmonyPatch]
	private static class InterceptLanguageLoad
	{
		private static MethodInfo TargetMethod() => AccessTools.DeclaredMethod(Type.GetType("Localization, assembly_guiutils"), "LoadLanguages");

		[HarmonyPriority(Priority.Last)]
		private static bool Prefix(ref List<string> __result)
		{
			Type localization = Type.GetType("Localization, assembly_guiutils")!;
			if (AccessTools.DeclaredField(localization, "m_instance").GetValue(null) is { } localizationInstance)
			{
				__result = (List<string>)AccessTools.DeclaredMethod(localization, "GetLanguages").Invoke(localizationInstance, Array.Empty<object>());
				return false;
			}

			return true;
		}
	}

	[HarmonyPatch]
	private static class InterceptChainloader
	{
		private static bool postChainloader = false;
		private static bool doNotSkipUpdate = false;
		private static readonly HashSet<MethodBase> methods = new();
		private static readonly MethodInfo patchSkip = AccessTools.DeclaredMethod(typeof(InterceptChainloader), nameof(SkipUpdates));
		private static readonly MethodInfo addReplacementOriginal = AccessTools.DeclaredMethod(typeof(PatchManager), "AddReplacementOriginal");

		private static MethodInfo TargetMethod() => postChainloader ? AccessTools.DeclaredMethod(Type.GetType("FejdStartup, assembly_valheim"), "Awake") : AccessTools.DeclaredMethod(typeof(Chainloader), nameof(Chainloader.Start));

		private static bool SkipUpdates(MethodBase original, ref MethodInfo? __result)
		{
			if (doNotSkipUpdate || (original.DeclaringType is { } type && passthroughClasses.Contains(type.FullName)))
			{
				return true;
			}

			methods.Add(original);
			__result = null;
			return false;
		}

		[HarmonyPriority(Priority.First)]
		public static void Prefix()
		{
			doNotSkipUpdate = false;
			delayedPatcherHarmony.Patch(harmonyPatcher, prefix: new HarmonyMethod(patchSkip));
		}

		[HarmonyPriority(Priority.Last)]
		public static void Postfix()
		{
			doNotSkipUpdate = true;
			string location;

			if (postChainloader)
			{
				delayedPatcherHarmony.UnpatchSelf();
				location = "after FejdStartup.Awake";

			}
			else
			{
				delayedPatcherHarmony.Unpatch(TargetMethod(), HarmonyPatchType.All, delayedPatcherHarmony.Id);
				postChainloader = true;
				delayedPatcherHarmony.PatchAll(typeof(InterceptChainloader));

				location = "after Chainloader end";
			}

			Stopwatch watch = new();
			watch.Start();
			lock (AccessTools.DeclaredField(typeof(PatchProcessor), "locker").GetValue(null))
			{
				foreach (MethodBase method in methods)
				{
					object methodInfo = harmonyPatcher.Invoke(null, new object[] { method, method.ToPatchInfo() });
					addReplacementOriginal.Invoke(null, new[] { method, methodInfo });
				}
			}

			logger.LogInfo($"Batch-patched {methods.Count} methods {location} in {watch.ElapsedMilliseconds} ms");
			methods.Clear();
		}
	}

	[HarmonyPatch]
	private static class DelayConfigSave
	{
		private static MethodBase TargetMethod() => AccessTools.DeclaredConstructor(typeof(ConfigFile), new[] { typeof(string), typeof(bool), typeof(BepInPlugin) });

		private static void Postfix(ConfigFile __instance)
		{
			__instance.SaveOnConfigSet = false;
			changedConfigFiles.Add(__instance);
		}
	}

	[HarmonyPatch]
	private static class ChangeConfigSaveBack
	{
		private static MethodInfo TargetMethod() => AccessTools.DeclaredMethod(Type.GetType("FejdStartup, assembly_valheim"), "Awake");

		private static void Postfix()
		{
			foreach (ConfigFile file in changedConfigFiles)
			{
				file.Save();
				file.SaveOnConfigSet = true;
			}

			changedConfigFiles.Clear();
		}
	}
}
