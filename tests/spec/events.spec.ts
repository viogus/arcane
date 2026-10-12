import { expect, test, type Page, type Route } from '../fixtures/test.fixture';

test.describe('Event log environment attribution', () => {
	const LOCAL_ENV_ID = '0';
	const LOCAL_ENV_NAME = 'Manager Daemon';
	const REMOTE_ENV_ID = 'remote-events-test';
	const REMOTE_ENV_NAME = 'Remote Lab';
	const REMOTE_EVENT_TITLE = 'Docker container create: remote-web';
	const LOCAL_EVENT_TITLE = 'Docker container create: local-web';

	type MockEvent = {
		id: string;
		type: string;
		severity: 'info' | 'warning' | 'error' | 'success';
		title: string;
		description?: string;
		resourceType?: string;
		resourceId?: string;
		resourceName?: string;
		environmentId?: string;
		timestamp: string;
		createdAt: string;
	};

	type EnvironmentFilterRequests = Array<string | null>;

	function response(data: unknown) {
		return { success: true, data };
	}

	function paginated<T>(data: T[]) {
		return {
			success: true,
			data,
			pagination: {
				totalPages: 1,
				totalItems: data.length,
				currentPage: 1,
				itemsPerPage: data.length,
				grandTotalItems: data.length
			}
		};
	}

	function event(
		id: string,
		title: string,
		environmentId: string | undefined,
		minutesAgo: number
	): MockEvent {
		const timestamp = new Date(Date.now() - minutesAgo * 60_000).toISOString();
		return {
			id,
			type: 'container.create',
			severity: 'info',
			title,
			description: `${title} observed from the Docker daemon`,
			resourceType: 'container',
			resourceId: `container-${id}`,
			resourceName: title.split(': ').at(-1) ?? id,
			environmentId,
			timestamp,
			createdAt: timestamp
		};
	}

	async function mockAppShell(page: Page) {
		await page.addInitScript(() => localStorage.removeItem('selectedEnvironmentId'));

		await page.route(/\/api\/auth\/me$/, async (route) => {
			await route.fulfill({
				json: response({
					id: 'events-admin',
					username: 'arcane',
					roleAssignments: [],
					permissionsByEnv: { global: ['*'] },
					isGlobalAdmin: true,
					createdAt: new Date().toISOString()
				})
			});
		});

		await page.route(/\/api\/auth\/auto-login-config$/, async (route) => {
			await route.fulfill({ json: response({ enabled: false }) });
		});

		await page.route(/\/api\/environments(?:\?.*)?$/, async (route) => {
			await route.fulfill({
				json: paginated([
					{
						id: LOCAL_ENV_ID,
						name: LOCAL_ENV_NAME,
						apiUrl: 'unix:///var/run/docker.sock',
						status: 'online',
						enabled: true,
						isEdge: false
					},
					{
						id: REMOTE_ENV_ID,
						name: REMOTE_ENV_NAME,
						apiUrl: 'tcp://remote.example.invalid:2375',
						status: 'online',
						enabled: true,
						isEdge: false
					}
				])
			});
		});

		await page.route(/\/api\/environments\/[^/]+\/settings(?:\/public)?$/, async (route) => {
			await route.fulfill({ json: response({}) });
		});

		await page.route(/\/api\/environments\/[^/]+\/swarm\/status$/, async (route) => {
			await route.fulfill({ json: response({ enabled: false }) });
		});

		await page.route(/\/api\/oidc\/status$/, async (route) => {
			await route.fulfill({ json: response({ envForced: false, envConfigured: false }) });
		});

		await page.route(/\/api\/roles\/available-permissions$/, async (route) => {
			await route.fulfill({ status: 503, json: { message: 'not needed for global admin' } });
		});

		await page.route(/\/api\/app-version$/, async (route) => {
			await route.fulfill({
				json: { currentVersion: 'e2e', displayVersion: 'e2e', enabledFeatures: [] }
			});
		});

		await page.route(/\/api\/stream(?:\?.*)?$/, () => {
			// Leave the stream pending: a quiet backend pushes no events, so the only thing
			// that can repaint the log after a filter change is the filter's own fetch.
		});
	}

	async function mockEvents(
		page: Page,
		events: MockEvent[],
		environmentIdRequests: EnvironmentFilterRequests
	) {
		await page.route(/\/api\/events\/stats$/, async (route) => {
			await route.fulfill({
				json: response({
					total: events.length,
					info: events.length,
					success: 0,
					warning: 0,
					error: 0
				})
			});
		});

		await page.route(/\/api\/events(?:\?.*)?$/, async (route: Route) => {
			const requested = new URL(route.request().url()).searchParams.get('environmentId');
			environmentIdRequests.push(requested);

			const wanted = (requested ?? '')
				.split(',')
				.map((value) => value.trim())
				.filter(Boolean);
			const visible = wanted.length
				? events.filter(
						(item) => item.environmentId !== undefined && wanted.includes(item.environmentId)
					)
				: events;

			await route.fulfill({ json: paginated(visible) });
		});
	}

	test('labels daemon events with their environment and filters the log by it', async ({
		page
	}) => {
		const environmentIdRequests: EnvironmentFilterRequests = [];
		const events = [
			event('event-remote', REMOTE_EVENT_TITLE, REMOTE_ENV_ID, 5),
			event('event-local', LOCAL_EVENT_TITLE, LOCAL_ENV_ID, 6)
		];

		await mockAppShell(page);
		await mockEvents(page, events, environmentIdRequests);

		await page.goto('/events');

		const remoteRow = page.getByRole('row').filter({ hasText: REMOTE_EVENT_TITLE }).first();
		const localRow = page.getByRole('row').filter({ hasText: LOCAL_EVENT_TITLE }).first();
		await expect(remoteRow).toBeVisible();
		await expect(localRow).toBeVisible();
		await expect(remoteRow).toContainText(REMOTE_ENV_NAME);
		await expect(localRow).toContainText(LOCAL_ENV_NAME);

		// Selecting an environment in the toolbar issues a filtered request...
		await page.getByTestId('facet-environment-trigger').click();
		await expect(page.getByTestId(`facet-environment-option-${REMOTE_ENV_ID}`)).toBeVisible();
		await page.getByTestId(`facet-environment-option-${REMOTE_ENV_ID}`).click();

		await expect.poll(() => environmentIdRequests).toContain(REMOTE_ENV_ID);

		// ...and the log only keeps the events attributed to it.
		await expect(remoteRow).toBeVisible();
		await expect(localRow).toHaveCount(0);

		// Clearing the filter brings every environment back.
		await page.getByTestId('facet-environment-trigger').click();
		await page.getByRole('option', { name: /clear/i }).click();
		await expect(localRow).toBeVisible();
	});

	test('keeps events without an environment filterable by the local daemon', async ({ page }) => {
		const environmentIdRequests: EnvironmentFilterRequests = [];
		const events = [event('event-local', LOCAL_EVENT_TITLE, undefined, 6)];

		await mockAppShell(page);
		await mockEvents(page, events, environmentIdRequests);

		await page.goto('/events');

		const localRow = page.getByRole('row').filter({ hasText: LOCAL_EVENT_TITLE }).first();
		await expect(localRow).toBeVisible();

		await page.getByTestId('facet-environment-trigger').click();
		await expect(page.getByTestId(`facet-environment-option-${LOCAL_ENV_ID}`)).toBeVisible();
	});
});
