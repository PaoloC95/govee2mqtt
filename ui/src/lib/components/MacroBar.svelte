<script lang="ts">
  import { listOneClicks, activateOneClick } from "../api";
  import type { OneClick } from "../types";
  import { Sparkles } from "@lucide/svelte";

  let items = $state<OneClick[]>([]);
  let loading = $state(false);
  let loaded = $state(false);
  let error = $state<string | null>(null);
  let firing = $state<string | null>(null);

  // Community authentication is only needed for Govee one-click macros.
  // Do not contact the undocumented Community API during normal dashboard
  // startup; load macros only after the user explicitly asks for them.
  async function loadMacros() {
    loading = true;
    error = null;
    try {
      items = await listOneClicks();
      loaded = true;
    } catch (e) {
      error = (e as Error).message;
    } finally {
      loading = false;
    }
  }

  async function fire(name: string) {
    firing = name;
    try {
      await activateOneClick(name);
    } catch (e) {
      console.error("one-click activate failed", e);
    } finally {
      firing = null;
    }
  }
</script>

<div class="flex items-center gap-2 overflow-x-auto py-1">
  {#if loading}
    <span class="text-xs text-zinc-500 select-none dark:text-zinc-400">loading macros...</span>
  {:else if error}
    <span class="font-mono text-xs text-red-600 select-none dark:text-red-400">macros: {error}</span>
    <button
      type="button"
      onclick={loadMacros}
      class="chip shrink-0 cursor-pointer px-3 py-1 text-xs"
    >
      retry
    </button>
  {:else if !loaded}
    <button
      type="button"
      onclick={loadMacros}
      class="chip shrink-0 cursor-pointer px-3 py-1 text-xs"
    >
      load one-click macros
    </button>
  {:else if items.length === 0}
    <span class="text-xs text-zinc-500 select-none dark:text-zinc-400"
      >no one-click macros configured in the govee app</span
    >
  {:else}
    {#each items as item, i (i)}
      <button
        type="button"
        onclick={() => fire(item.name)}
        disabled={firing === item.name}
        class="chip inline-flex shrink-0 cursor-pointer items-center gap-1.5 rounded-full! px-3 py-1 text-xs font-medium transition-colors select-none hover:bg-white/85 disabled:cursor-not-allowed disabled:opacity-50 dark:hover:bg-zinc-800/60"
      >
        <Sparkles class="size-3.5 text-amber-500 dark:text-amber-400" />
        {item.name}
      </button>
    {/each}
  {/if}
</div>
