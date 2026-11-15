'use strict';

// Reads existing Git credentials in memory; never prints or persists credentials or job logs.
const {execFileSync} = require('node:child_process');
async function main() {
  const [repository = 'fjiangming/sub2api',sha,runId] = process.argv.slice(2);
  if (!/^fjiangming\/(?:sub2api|sub2api-extra)$/.test(repository) || sha && !/^[a-f0-9]{40}$/.test(sha) || runId && !/^\d+$/.test(runId)) throw new Error('Invalid status target');
  let token = process.env.GH_TOKEN || process.env.GITHUB_TOKEN;
  if (!token) {
    try {
      const credential = execFileSync('git',['credential','fill'],{input:'protocol=https\nhost=github.com\n\n',encoding:'utf8',stdio:['pipe','pipe','pipe'],env:{...process.env,GIT_TERMINAL_PROMPT:'0',GCM_INTERACTIVE:'Never'}});
      token = credential.split(/\r?\n/).find(line => line.startsWith('password='))?.slice(9);
    } catch { /* Public status still works when the anonymous API budget permits. */ }
  }
  async function api(path) {
    const response = await fetch('https://api.github.com/repos/' + repository + path,{headers:{'User-Agent':'sub2api-dev-status',...(token ? {authorization:'Bearer ' + token} : {})},signal:AbortSignal.timeout(30000)});
    if (!response.ok) throw new Error('GitHub API status ' + response.status);
    return response.json();
  }
  const runs = await api('/actions/runs?' + new URLSearchParams({...(sha ? {head_sha:sha} : {branch:'dev'}),per_page:'12'}));
  console.log(JSON.stringify({repository,runs:runs.workflow_runs.map(row => ({id:row.id,name:row.name,sha:row.head_sha,status:row.status,conclusion:row.conclusion,url:row.html_url}))}));
  if (runId) {
    const jobs = await api(`/actions/runs/${runId}/jobs?per_page=100`);
    console.log(JSON.stringify({jobs:jobs.jobs.map(row => ({id:row.id,name:row.name,status:row.status,conclusion:row.conclusion,failedSteps:row.steps.filter(step => step.conclusion === 'failure').map(step => step.name)}))}));
  }
  const release = await api('/releases/latest');
  console.log(JSON.stringify({release:{tag:release.tag_name,target:release.target_commitish,url:release.html_url,assets:release.assets.map(asset => ({name:asset.name,bytes:asset.size,digest:asset.digest}))}}));
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
