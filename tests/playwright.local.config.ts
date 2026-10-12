import { defineConfig, devices } from '@playwright/test';

// Local harness: runs the mocked specs against a statically served frontend build
// (no docker compose stack, no auth/gitops setup projects, no backend).
const baseURL = process.env.BASE_URL || 'http://127.0.0.1:4399';

export default defineConfig({
	testDir: '.',
	outputDir: '/tmp/pw-local-results',
	fullyParallel: false,
	workers: 1,
	reporter: [['line']],
	use: {
		baseURL,
		serviceWorkers: 'block',
		trace: 'retain-on-failure',
		screenshot: 'only-on-failure'
	},
	projects: [
		{
			name: 'chromium',
			// Use the locally installed Chrome instead of a bundled download.
			use: { ...devices['Desktop Chrome'], channel: 'chrome' },
			testMatch: '**/spec/*.spec.ts',
			testIgnore: [
				'**/spec/cli.spec.ts',
				'**/spec/responsive.spec.ts',
				'**/spec/accessibility.spec.ts'
			]
		}
	]
});
