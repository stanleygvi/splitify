import { execSync } from 'node:child_process';

const platform = process.platform;
const arch = process.arch;

const packageByTarget = {
  'darwin-arm64': '@rollup/rollup-darwin-arm64',
  'darwin-x64': '@rollup/rollup-darwin-x64',
  'linux-arm64': '@rollup/rollup-linux-arm64-gnu',
  'linux-x64': '@rollup/rollup-linux-x64-gnu',
  'win32-x64': '@rollup/rollup-win32-x64-msvc',
};

const target = `${platform}-${arch}`;
const nativePackage = packageByTarget[target];

if (!nativePackage) {
  console.log(`[ensure-rollup-native] No explicit native package mapping for ${target}.`);
  process.exit(0);
}

try {
  await import(nativePackage);
  console.log(`[ensure-rollup-native] Native rollup package present: ${nativePackage}`);
} catch {
  console.log(`[ensure-rollup-native] Installing missing native rollup package: ${nativePackage}`);
  execSync(`npm install --no-save ${nativePackage}@4.59.0`, { stdio: 'inherit' });
}
