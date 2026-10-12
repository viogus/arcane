<script lang="ts">
	import { createQuery } from '@tanstack/svelte-query';

	import { UpdateIcon } from '#lib/icons/index.js';
	import { m } from '#lib/paraglide/messages.js';
	import { queryKeys } from '#lib/query/query-keys.js';
	import systemUpgradeService from '#lib/services/api/system-upgrade-service.js';
	import { systemService } from '#lib/services/system-service.js';
	import type { Environment } from '#lib/types/environment.js';

	let { environment, isOnline }: { environment: Environment; isOnline: boolean } = $props();

	// A docker-host environment runs no Arcane of its own, so the version that belongs in
	// this row is the Docker engine version the manager talks to directly.
	const isDockerHost = $derived(Boolean(environment.dockerHost));

	// Shares its key with the upgrade menu item, so opening a row's actions reuses this
	// result instead of asking the agent again. Offline environments are never queried:
	// remote version lookups are proxied through the agent and would just time out.
	const versionQuery = createQuery(() => ({
		queryKey: queryKeys.system.versionInfo(environment.id),
		queryFn: () => systemUpgradeService.getVersionInfo(environment.id),
		enabled: isOnline && !isDockerHost,
		retry: false
	}));

	const dockerInfoQuery = createQuery(() => ({
		queryKey: queryKeys.system.dockerInfo(environment.id),
		queryFn: () => systemService.getDockerInfoForEnvironment(environment.id),
		enabled: isOnline && isDockerHost,
		retry: false
	}));

	const info = $derived(versionQuery.data ?? null);
	const agentLabel = $derived(info?.displayVersion || info?.currentVersion || info?.currentTag || '');
	const dockerLabel = $derived(dockerInfoQuery.data?.ServerVersion ?? '');
	const label = $derived(isDockerHost ? dockerLabel : agentLabel);
	const labelTitle = $derived(isDockerHost ? m.common_docker_version() : undefined);
</script>

{#if label}
	<div class="flex items-center gap-1.5">
		<span class="font-mono text-sm" title={labelTitle}>{label}</span>
		{#if !isDockerHost && info?.updateAvailable}
			<span
				class="inline-flex items-center text-warning"
				title={info.newestVersion
					? m.sidebar_update_available_tooltip({ version: info.newestVersion })
					: m.sidebar_update_available()}
			>
				<UpdateIcon class="size-3.5" />
			</span>
		{/if}
	</div>
{:else}
	<span class="text-sm text-muted-foreground">—</span>
{/if}
