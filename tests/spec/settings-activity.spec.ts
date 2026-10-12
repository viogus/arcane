import { expect, test, type Page } from '../fixtures/test.fixture';

test.describe('Activity settings content', () => {
	const LOCAL_ENV_ID = '0';

	// The settings endpoints return the raw key/value list; the app normalizes it.
	type SettingEntry = { key: string; type: string; value: string };

	const SETTINGS_ENTRIES: SettingEntry[] = [
		{ key: 'activityHistoryRetentionDays', type: '', value: '45' },
		{ key: 'activityHistoryMaxEntries', type: '', value: '2500' },
		{ key: 'maxConcurrentActivities', type: '', value: '9' },
		{ key: 'upgradeLogRetentionDays', type: '', value: '11' },
		{ key: 'uiConfigDisabled', type: 'boolean', value: 'false' }
	];

	function response(data: unknown) {
		return { success: true, data };
	}

	async function mockAppShell(page: Page, settings: SettingEntry[]) {
		await page.addInitScript(() => localStorage.removeItem('selectedEnvironmentId'));

		await page.route(/\/api\/auth\/me$/, async (route) => {
			await route.fulfill({
				json: response({
					id: 'activity-admin',
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
				json: {
					success: true,
					data: [
						{
							id: LOCAL_ENV_ID,
							name: 'Manager Daemon',
							apiUrl: 'unix:///var/run/docker.sock',
							status: 'online',
							enabled: true,
							isEdge: false
						}
					],
					pagination: {
						totalPages: 1,
						totalItems: 1,
						currentPage: 1,
						itemsPerPage: 1,
						grandTotalItems: 1
					}
				}
			});
		});

		await page.route(/\/api\/environments\/0\/settings$/, async (route) => {
			await route.fulfill({ json: settings });
		});

		await page.route(/\/api\/environments\/0\/settings\/public$/, async (route) => {
			await route.fulfill({ json: settings });
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

		await page.route(/\/api\/stream(?:\?.*)?$/, async (route) => {
			await route.fulfill({ contentType: 'text/event-stream', body: '' });
		});
	}

	test('shows the configured retention values for the environment', async ({ page }) => {
		await mockAppShell(page, SETTINGS_ENTRIES);

		await page.goto('/settings/activity');

		await expect(page.getByRole('heading', { name: 'Activity', exact: true })).toBeVisible();
		await expect(
			page.getByText('Configure Activity Center history retention for each environment.')
		).toBeVisible();
		await expect(
			page.getByRole('heading', { name: 'History Retention', exact: true })
		).toBeVisible();
		await expect(page.getByRole('heading', { name: 'Upgrade logs', exact: true })).toBeVisible();

		await expect(page.locator('label[for="activity-history-retention-days"]')).toHaveText(
			'Retention Days'
		);
		await expect(page.locator('label[for="activity-history-max-entries"]')).toHaveText(
			'Maximum Entries'
		);
		await expect(page.locator('label[for="activity-max-concurrent"]')).toHaveText(
			'Concurrent Activity Limit'
		);
		await expect(page.locator('label[for="upgrade-log-retention-days"]')).toHaveText(
			'Upgrade log retention (days)'
		);

		await expect(page.locator('#activity-history-retention-days')).toHaveValue('45');
		await expect(page.locator('#activity-history-max-entries')).toHaveValue('2500');
		await expect(page.locator('#activity-max-concurrent')).toHaveValue('9');
		await expect(page.locator('#upgrade-log-retention-days')).toHaveValue('11');
	});

	test('falls back to the default upgrade log retention when the environment omits it', async ({
		page
	}) => {
		await mockAppShell(
			page,
			SETTINGS_ENTRIES.filter((entry) => entry.key !== 'upgradeLogRetentionDays')
		);

		await page.goto('/settings/activity');

		await expect(page.locator('#upgrade-log-retention-days')).toHaveValue('3');
	});
});
