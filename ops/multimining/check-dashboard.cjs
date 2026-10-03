// Isolated browser fixtures: no node, wallet storage or real payout configuration.
const {chromium}=require('playwright');
const http=require('node:http'),fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../bridge/static');
// Synthetic address encodings, not wallet addresses.
const zkas='zkas:p9pyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyyssmdvpfyna';
const kaspa='kaspa:qqjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgtturx5zd';
const modes=[
 {instance:'[Instance 1]',mode:'native',port:5555,parentPrefix:null},
 {instance:'[Instance 2]',mode:'kaspa',port:5556,parentPrefix:'kaspa'},
];
const stats={totalBlocks:0,totalShares:0,networkHashrate:0,networkDifficulty:0,networkBlockCount:0,activeWorkers:0,internalCpu:null,blocks:[],workers:[],bridgeUptime:0};
(async()=>{
 let failMetadata=false;
 const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost');
  if(url.pathname==='/api/mining'&&failMetadata){res.writeHead(503);return res.end();}
  const fixture={'/api/mining':modes,'/api/status':{kaspad_address:'test fixture',kaspad_version:'fixture',instances:2},'/api/stats':stats}[url.pathname];
  if(fixture){res.setHeader('Content-Type','application/json');return res.end(JSON.stringify(fixture));}
  const rel=url.pathname==='/'?'index.html':url.pathname.replace(/^\/static\//,'');
  const file=path.resolve(root,rel);
  if(!file.startsWith(root+path.sep)||!fs.existsSync(file)){res.writeHead(404);return res.end();}
  res.setHeader('Content-Type',file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':file.endsWith('.png')?'image/png':file.endsWith('.svg')?'image/svg+xml':'text/html');
  res.end(fs.readFileSync(file));
 });
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 let browser;
 try{
  browser=await chromium.launch({headless:true,args:['--no-sandbox']});
  const page=await browser.newPage({viewport:{width:1280,height:1000}});
  const errors=[];page.on('pageerror',e=>errors.push(String(e)));
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  await page.waitForFunction(()=>document.querySelectorAll('#miningMode option').length===2);
  await page.locator('#miningHost').fill('pool.example.com');
  await page.locator('[name=zkas]').fill(zkas);
  await page.locator('[name=worker]').fill('asic1');
  assert.equal(await page.locator('#miningPassword').textContent(),'x');
  await page.locator('#miningMode').selectOption('1');
  assert.equal(await page.locator('#miningCredentials').isVisible(),false);
  await page.locator('[name=kaspa]').fill(kaspa);
  assert.equal(await page.locator('#miningUrl').textContent(),'stratum+tcp://pool.example.com:5556');
  assert.equal(await page.locator('#miningPassword').textContent(),kaspa);
  assert.equal(await page.locator('#miningUsername').textContent(),zkas+'.asic1');
  await page.locator('[name=kaspa]').fill(kaspa.slice(0,-1)+'q');
  assert.equal(await page.locator('#miningCredentials').isVisible(),false);
  assert.equal(await page.locator('#miningCopy').isDisabled(),true);
  await page.locator('#miningMode').selectOption('0');
  assert.equal(await page.locator('#miningPassword').textContent(),'x');
  await page.setViewportSize({width:390,height:844});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  failMetadata=true;await page.reload();
  await page.waitForFunction(()=>document.getElementById('miningSetupError').textContent.includes('unavailable'));
  assert.equal(await page.locator('#miningCopy').isDisabled(),true);
  assert.deepEqual(errors,[]);
  console.log('PASS: native/Kaspa, address rejection, no stale credentials, mobile layout, metadata outage, no page exceptions');
 }finally{await browser?.close();await new Promise(r=>server.close(r));}
})().catch(e=>{console.error(e);process.exitCode=1;});
