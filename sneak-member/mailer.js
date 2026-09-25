import {sendTransactionalEmail} from '../sneak-shared/email-provider.js';
export async function handleInternalMail(request,env){
 const token=request.headers.get('Authorization')||'';
 const expected=env.SNEAK_MAILER_SECRET?'Bearer '+env.SNEAK_MAILER_SECRET:'';
 if(!expected||token.length!==expected.length)return new Response('Unauthorized',{status:401});
 let diff=0;for(let i=0;i<token.length;i++)diff|=token.charCodeAt(i)^expected.charCodeAt(i);
 if(diff)return new Response('Unauthorized',{status:401});
 const raw=await request.text();if(raw.length>100000)return new Response('Too large',{status:413});
 let payload;try{payload=JSON.parse(raw);}catch{return new Response('Invalid JSON',{status:400});}
 if(!payload||typeof payload.to!=='string'||!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(payload.to)||typeof payload.subject!=='string'||payload.subject.length>250||typeof payload.html!=='string')return new Response('Invalid message',{status:400});
 const result=await sendTransactionalEmail(env,{...payload,from:env.EMAIL_FROM});
 return Response.json(result,{headers:{'Cache-Control':'no-store'}});
}
