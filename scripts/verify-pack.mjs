import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: 'utf8' });
  if (res.status !== 0) {
    throw new Error(`${cmd} ${args.join(' ')} failed (exit ${res.status}):\n${res.stdout}\n${res.stderr}`);
  }
  return res.stdout;
}

const repoRoot = resolve(import.meta.dirname, '..');
const tscBin = resolve(repoRoot, 'node_modules', '.bin', 'tsc');
const work = mkdtempSync(join(tmpdir(), 'magnetite-pack-'));

try {
  const packOut = run('npm', ['pack', '--pack-destination', work], repoRoot);
  const tarballName = packOut.trim().split(/\r?\n/).at(-1);
  assert.ok(tarballName && tarballName.endsWith('.tgz'), `unexpected npm pack output: ${packOut}`);
  const tarballPath = join(work, tarballName);

  const entries = run('tar', ['-tzf', tarballPath], repoRoot)
    .trim()
    .split(/\r?\n/)
    .filter(Boolean);
  console.log('Packed files:\n' + entries.join('\n'));

  for (const required of [
    'package/package.json',
    'package/README.md',
    'package/LICENSE',
    'package/dist/index.js',
    'package/dist/index.d.ts',
    'package/dist/core.js',
    'package/dist/core.d.ts',
  ]) {
    assert.ok(entries.includes(required), `missing expected tarball entry: ${required}`);
  }
  for (const entry of entries) {
    assert.ok(!entry.includes('model'), `unexpected model file in tarball: ${entry}`);
    assert.ok(!entry.startsWith('package/test/'), `unexpected test file in tarball: ${entry}`);
    assert.ok(!entry.startsWith('package/scripts/'), `unexpected script file in tarball: ${entry}`);
  }

  const consumer = join(work, 'consumer');
  mkdirSync(consumer, { recursive: true });
  writeFileSync(
    join(consumer, 'package.json'),
    JSON.stringify({ name: 'magnetite-pack-consumer', private: true, type: 'module' }, null, 2),
  );
  run('npm', ['install', '--no-audit', '--no-fund', tarballPath], consumer);

  writeFileSync(
    join(consumer, 'check.mjs'),
    `import assert from 'node:assert/strict';
import * as root from '@minamorl/magnetite';
import * as core from '@minamorl/magnetite/core';

for (const name of ['Client', 'connect', 'Server', 'ServerConnection', 'Core', 'issueIdentifier', 'systemClock']) {
  assert.ok(name in root, 'missing root export: ' + name);
}
for (const name of ['Core', 'issueIdentifier']) {
  assert.ok(name in core, 'missing core export: ' + name);
}
assert.equal(root.Core, core.Core);
assert.equal(root.issueIdentifier, core.issueIdentifier);
const id = core.issueIdentifier(1, (n) => new Uint8Array(n).fill(255));
assert.equal(id, '00000000-0001-7fff-bfff-ffffffffffff');
console.log('pack runtime import check ok');
`,
  );
  console.log(run(process.execPath, ['check.mjs'], consumer).trim());

  writeFileSync(
    join(consumer, 'check.ts'),
    `import {
  Client,
  connect,
  Server,
  ServerConnection,
  Core,
  issueIdentifier,
  systemClock,
} from '@minamorl/magnetite';
import type {
  ClientOptions,
  ServerOptions,
  Clock,
  SocketLike,
  Callbacks,
  Timing,
  Role,
  State,
  Json,
  Frame,
  EndReason,
  Effect,
  RecordData,
  CoreEvent,
  CoreOptions,
  Result,
} from '@minamorl/magnetite';
import { Core as SubCore, issueIdentifier as subIssue } from '@minamorl/magnetite/core';
import type { Event, Options } from '@minamorl/magnetite/core';

const c: typeof Core = SubCore;
const fn: typeof issueIdentifier = subIssue;
const clientOpts: ClientOptions = { clock: systemClock };
const serverOpts: ServerOptions = {};
export const values = [Client, connect, Server, ServerConnection, c, fn, clientOpts, serverOpts];
export type Types = [
  Clock,
  SocketLike,
  Callbacks,
  Timing,
  Role,
  State,
  Json,
  Frame,
  EndReason,
  Effect,
  RecordData,
  CoreEvent,
  CoreOptions,
  Result,
  Event,
  Options,
];
`,
  );
  run(
    tscBin,
    ['--noEmit', '--strict', '--target', 'ES2022', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', 'check.ts'],
    consumer,
  );
  console.log('pack typecheck ok');
} finally {
  rmSync(work, { recursive: true, force: true });
}
