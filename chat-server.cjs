// Node.js 18+; no packages needed. Rooms and messages live in memory.
const http=require('node:http'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const countries=new Set(['Brazil','China','India','Lebanon','Mexico','Morocco','Pakistan','Philippines','Russia','South Africa','Tanzania','Thailand','Turkey','Vietnam']);
const sessions=new Map(),rooms=new Map();
const roomFor=c=>{if(!rooms.has(c))rooms.set(c,{messages:[],clients:new Map()});return rooms.get(c)};
const emit=(res,data)=>res.write('data: '+JSON.stringify(data)+'\n\n');
function broadcast(room,data){for(const res of room.clients.values())emit(res,data)}
function presence(room){broadcast(room,{type:'presence',count:room.clients.size})}
function leave(token){const s=sessions.get(token);if(!s)return;const room=roomFor(s.country),res=room.clients.get(token);room.clients.delete(token);sessions.delete(token);res?.end();presence(room)}
function json(res,status,data){res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(data))}
async function body(req){let text='';for await(const chunk of req){text+=chunk;if(text.length>4096)throw Error('Too large')}return JSON.parse(text)}
const server=http.createServer(async(req,res)=>{const url=new URL(req.url,'http://localhost');res.setHeader('X-Content-Type-Options','nosniff');
 try{
  if(req.method==='GET'&&(url.pathname==='/'||url.pathname==='/cultural-learning-dashboard-v2.html')){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});fs.createReadStream(path.join(__dirname,'cultural-learning-dashboard-v2.html')).pipe(res);return}
  if((req.method==='GET'||req.method==='HEAD')&&url.pathname.startsWith('/storybooks/')){
   const name=decodeURIComponent(url.pathname.slice('/storybooks/'.length));
   if(!name||name.includes('/')||name.includes('\\')||name.includes('\0')||!name.toLowerCase().endsWith('.pdf'))return json(res,400,{error:'Invalid PDF name'});
   const root=path.join(__dirname,'storybooks'),file=path.join(root,name);
   let stat;try{const real=await fs.promises.realpath(file);if(path.dirname(real)!==await fs.promises.realpath(root))return json(res,403,{error:'Invalid PDF path'});stat=await fs.promises.stat(real);if(!stat.isFile())throw Error()}catch{return json(res,404,{error:'PDF missing. Upload this exact filename into the storybooks folder: '+name})}
   res.writeHead(200,{'Content-Type':'application/pdf','Content-Length':stat.size,'Cache-Control':'public, max-age=3600'});
   if(req.method==='HEAD')return res.end();
   const stream=fs.createReadStream(file);stream.on('error',()=>res.destroy());stream.pipe(res);return;
  }
  if(req.method==='POST'&&url.pathname.startsWith('/api/chat/')){
   if(req.headers.origin&&new URL(req.headers.origin).host!==req.headers.host)return json(res,403,{error:'Different origin'});
   const data=await body(req);
   if(url.pathname==='/api/chat/join'){
    if(!countries.has(data.country)||typeof data.name!=='string'||!data.name.trim()||data.name.length>80)return json(res,400,{error:'Invalid country or nickname'});
    if(sessions.size>=500)return json(res,503,{error:'Rooms full'});
    const token=crypto.randomBytes(24).toString('hex');sessions.set(token,{country:data.country,name:data.name.trim(),created:Date.now(),last:0});return json(res,200,{token});
   }
   const s=sessions.get(data.token);if(!s)return json(res,401,{error:'Join again'});
   if(url.pathname==='/api/chat/leave'){leave(data.token);return json(res,200,{ok:true})}
   if(url.pathname==='/api/chat/messages'){
    const room=roomFor(s.country);if(!room.clients.has(data.token))return json(res,409,{error:'Connect first'});
    if(typeof data.text!=='string'||!data.text.trim()||data.text.length>500)return json(res,400,{error:'Use 1–500 characters'});
    if(Date.now()-s.last<1000)return json(res,429,{error:'Wait one second'});s.last=Date.now();
    const message={id:crypto.randomUUID(),name:s.name,text:data.text.trim(),time:Date.now()};room.messages.push(message);room.messages=room.messages.slice(-100);broadcast(room,{type:'message',message});return json(res,200,{ok:true});
   }
  }
  if(req.method==='GET'&&url.pathname==='/api/chat/events'){
   const token=url.searchParams.get('token'),s=sessions.get(token);if(!s)return json(res,401,{error:'Join again'});
   const room=roomFor(s.country);if(room.clients.has(token))return json(res,409,{error:'Already connected'});
   res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache, no-transform','Connection':'keep-alive','X-Accel-Buffering':'no'});res.write(': connected\n\n');room.clients.set(token,res);emit(res,{type:'history',messages:room.messages});presence(room);
   const beat=setInterval(()=>res.write(': heartbeat\n\n'),20000);res.on('close',()=>{clearInterval(beat);if(room.clients.get(token)===res){room.clients.delete(token);sessions.delete(token);presence(room)}});return;
  }
  json(res,404,{error:'Not found'});
 }catch{if(!res.headersSent)json(res,400,{error:'Invalid request'});else res.end()}
});
setInterval(()=>{for(const [token,s] of sessions)if(!roomFor(s.country).clients.has(token)&&Date.now()-s.created>60000)leave(token)},30000).unref();
const port=Number(process.env.PORT)||3000,host=process.env.HOST||'127.0.0.1';server.listen(port,host,()=>console.log('Dashboard: http://'+host+':'+port));
