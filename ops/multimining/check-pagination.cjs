// Synthetic browser data. No node, pool credentials or real payout addresses.
const {chromium}=require('playwright');
const http=require('node:http'),fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../bridge/static');
const now=Math.floor(Date.now()/1000);
const blocks=Array.from({length:5000},(_,i)=>({instance:'fixture',worker:`worker-${i%120}`,wallet:i%2?'fixture-b':'fixture-a',timestamp:String(now-i),hash:i.toString(16).padStart(64,'0'),nonce:String(i),bluescore:String(9000-i)}));
const workers=Array.from({length:120},(_,i)=>({instance:'fixture',worker:`worker-${i}`,wallet:i%2?'fixture-b':'fixture-a',hashrate:1000,shares:100,stale:0,invalid:0,blocks:1,currentDifficulty:4096,sessionUptime:900,status:'online',lastSeen:now}));
let stats={totalBlocks:5000,totalShares:12000,networkHashrate:1e15,networkDifficulty:100,networkBlockCount:10,activeWorkers:121,internalCpu:{hashrateGhs:1,blocksAccepted:1,wallet:'fixture-c'},blocks,workers,bridgeUptime:900,poolHashrate:120001e9,hashrateWindowSeconds:300};
let offline=false,delay=0,inflight=0,maxInflight=0;
(async()=>{
 const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost');
  if(url.pathname.startsWith('/api/')){
   if(offline){res.writeHead(503);return res.end();}
   if(url.pathname==='/api/stats'){
    inflight++;maxInflight=Math.max(maxInflight,inflight);
    return setTimeout(()=>{inflight--;res.setHeader('Content-Type','application/json');res.end(JSON.stringify(stats));},delay);
   }
   res.setHeader('Content-Type','application/json');return res.end(JSON.stringify(url.pathname==='/api/mining'?[]:{kaspad_version:'fixture',instances:1}));
  }
  const file=path.resolve(root,url.pathname==='/'?'index.html':url.pathname.replace(/^\/static\//,''));
  if(!file.startsWith(root+path.sep)||!fs.existsSync(file)){res.writeHead(404);return res.end();}
  res.setHeader('Content-Type',file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':'text/html');res.end(fs.readFileSync(file));
 });
 await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
 try{
  browser=await chromium.launch({headless:true,args:['--no-sandbox']});
  const page=await browser.newPage({viewport:{width:1280,height:1000},acceptDownloads:true,hasTouch:true});
  const errors=[];page.on('pageerror',e=>errors.push(String(e)));
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  await page.waitForFunction(()=>document.getElementById('blocksPageInfo').textContent.includes('of 5000'));
  assert.equal(await page.locator('#blocksBody tr').count(),25);
  assert.equal(await page.locator('#workersBody tr').count(),25);
  assert.equal(await page.locator('#totalWorkerHashrate').textContent(),'(120.00 TH/s)');
  await page.locator('#blocksNext').click();
  assert.equal(await page.locator('#blocksBody tr').first().getAttribute('data-row-index'),'25');
  await page.locator('#blocksBody tr').first().locator('td').first().click();
  await page.waitForFunction(()=>!document.getElementById('rowDetailModal').classList.contains('hidden'));
  assert.ok((await page.locator('#rowDetailBody').textContent()).includes(blocks[25].hash));
  await page.locator('#rowDetailClose').click();
  await page.locator('#workersNext').click();
  assert.equal(await page.locator('#workersBody tr').first().getAttribute('data-row-index'),'24');
  assert.equal(await page.locator('#workersBody tr').first().locator('td').count(),12);
  await page.evaluate(()=>refresh());
  assert.ok((await page.locator('#blocksPageInfo').textContent()).startsWith('Page 2'));
  assert.ok((await page.locator('#workersPageInfo').textContent()).startsWith('Page 2'));
  await page.locator('#blocksPageSize').selectOption('100');
  assert.equal(await page.locator('#blocksBody tr').count(),100);
  await page.locator('#walletSearchInput').fill('fixture-a');await page.locator('#walletSearchBtn').click();
  await page.waitForFunction(()=>document.getElementById('blocksPageInfo').textContent.includes('of 2500'));
  assert.ok((await page.locator('#blocksPageInfo').textContent()).startsWith('Page 1'));
  assert.equal(await page.locator('#workersBody tr[data-row-kind=icpu]').count(),0);
  assert.ok((await page.locator('#workersPageInfo').textContent()).includes('of 60'));
  const download=page.waitForEvent('download');await page.locator('#downloadBlocksCsv').click();
  const csv=fs.readFileSync(await (await download).path(),'utf8');assert.equal(csv.trim().split(/\r?\n/).length,2501);
  // Shrinking live data clamps the current page.
  await page.locator('#blocksNext').click();stats={...stats,blocks:[],workers:[],internalCpu:null,poolHashrate:0};
  await page.evaluate(()=>cacheClear());await page.evaluate(()=>refresh());
  assert.equal(await page.locator('#blocksBody tr').count(),0);assert.equal(await page.locator('#blocksNext').isDisabled(),true);
  stats={...stats,blocks,workers};await page.locator('#walletClearBtn').click();
  await page.waitForFunction(()=>document.getElementById('blocksPageInfo').textContent.includes('of 5000'));
  offline=true;await page.reload();
  await page.waitForFunction(()=>document.querySelectorAll('#blocksBody tr').length===25);
  assert.equal(await page.locator('#workersBody tr').first().locator('td').count(),12);
  await page.locator('#blocksNext').click();assert.equal(await page.locator('#blocksBody tr').first().getAttribute('data-row-index'),'25');
  await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  offline=false;delay=3500;await page.evaluate(()=>{refresh();refresh();refresh();});await page.waitForTimeout(4000);assert.equal(maxInflight,1);
  assert.deepEqual(errors,[]);
  console.log('PASS: 5000 blocks, 120 workers, CPU pagination, absolute detail indices, page retention/clamping, full CSV, filters, offline rendering, mobile, single refresh in flight');
 }finally{await browser?.close();await new Promise(r=>server.close(r));}
})().catch(e=>{console.error(e);process.exitCode=1;});
