import { descriptions } from '../test/model.ts';
import { writeFileSync } from 'node:fs';
const phases=['preparing','open','disconnected','closed'];
const events=Object.keys(descriptions.preparing);
let text='# 状態遷移表\n\n`node docs/generate.mjs` で test/model.ts の実装済み matrix から生成。各行はイベントを処理する側。出来事は一列で処理する。\n\n| 出来事 | 準備中 | 開 | 切断中 | 閉 |\n|---|---|---|---|---|\n';
for(const event of events) text+=`| ${event} | ${phases.map(p=>descriptions[p][event]).join(' | ')} |\n`;
text+=`\n${events.length} events × ${phases.length} states = ${events.length*phases.length} cells; empty: ${phases.flatMap(p=>events.map(e=>descriptions[p][e])).filter(x=>!x).length}\n`;
writeFileSync(new URL('./table.md',import.meta.url),text);
