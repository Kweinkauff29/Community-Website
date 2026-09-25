import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync,readdirSync} from 'node:fs';
import {contactsApi,weeklyWindow,processOwnerNotifications,renderOwnerNotification} from '../sneak-shared/contacts.js';
import {handleInternalMail} from '../sneak-member/mailer.js';
import {contactsDashboardScript} from '../sneak-shared/contacts-ui.js';
import {handleHostedPortal} from '../SneakIDXWorker.js';
function fixture(){
 const sql=new DatabaseSync(':memory:');sql.exec('PRAGMA foreign_keys=ON');
 for(const file of readdirSync(new URL('../migrations/',import.meta.url)).sort())if(file.endsWith('.sql'))sql.exec(readFileSync(new URL('../migrations/'+file,import.meta.url),'utf8'));
 for(const suffix of ['a','b']){
  sql.prepare('INSERT INTO sneak_accounts(id,account_name) VALUES(?,?)').run(suffix,'Team '+suffix);
  sql.prepare('INSERT INTO sneak_account_entitlements(account_id,status) VALUES(?,?)').run(suffix,'active');
  sql.prepare('INSERT INTO sneak_sites(id,account_id,site_key,site_name) VALUES(?,?,?,?)').run(suffix,suffix,suffix,'Site '+suffix);
  sql.prepare('INSERT INTO sneak_member_users(id,account_id,email,status) VALUES(?,?,?,?)').run(suffix,suffix,suffix+'@example.com','active');
 }
 const db={prepare(query){let values=[];const stmt={bind(...args){values=args;return stmt;},async first(){return sql.prepare(query).get(...values)||null;},async all(){return {results:sql.prepare(query).all(...values)};},async run(){const r=sql.prepare(query).run(...values);return {meta:{changes:Number(r.changes)}};}};return stmt;},async batch(statements){sql.exec('BEGIN');try{const out=[];for(const s of statements)out.push(await s.run());sql.exec('COMMIT');return out;}catch(e){sql.exec('ROLLBACK');throw e;}}};
 return {sql,db};
}
function inquiry(sql,id='l1',site='a',email='buyer@example.com'){sql.prepare("INSERT INTO sneak_leads(id,site_id,name,email,message,lead_type) VALUES(?,?,?,?,?,'schedule_tour')").run(id,site,'Buyer',email,'Please show me the home');}
function login(sql,id='u1',site='a',email='buyer@example.com'){sql.prepare('INSERT INTO sneak_consumer_users(id,site_id,email) VALUES(?,?,?)').run(id,site,email);sql.prepare("UPDATE sneak_consumer_users SET status='active',activated_at='2026-09-25T10:00:00Z',last_login_at='2026-09-25T10:00:00Z' WHERE id=?").run(id);}
const env={OWNER_NOTIFICATIONS_ENABLED:'true',SNEAK_MAILER_SECRET:'test-secret',MAILER:{fetch:async()=>Response.json({success:true,providerMessageId:'123'})}};
test('canonical migration and triggers merge inquiries with verified login only within a tenant',()=>{
 const {sql}=fixture();try{inquiry(sql);login(sql);login(sql,'u2','b');
 assert.equal(sql.prepare('SELECT count(*) n FROM sneak_contacts').get().n,2);
 const c=sql.prepare("SELECT * FROM sneak_contacts WHERE site_id='a'").get();assert.equal(c.consumer_id,'u1');assert.equal(c.name,'Buyer');
 sql.exec("UPDATE sneak_consumer_users SET last_login_at='2026-09-25T11:00:00Z' WHERE id='u1'");
 assert.equal(sql.prepare("SELECT count(*) n FROM sneak_owner_notifications WHERE kind='signup' AND site_id='a'").get().n,1);
 assert.equal(sql.prepare("SELECT count(*) n FROM sneak_contact_events WHERE event_type='login' AND site_id='a'").get().n,2);
 assert.deepEqual(sql.prepare('PRAGMA foreign_key_check').all(),[]);
 }finally{sql.close();}
});
test('contacts API isolates details/settings and blocks viewer writes',async()=>{
 const {sql,db}=fixture();try{inquiry(sql);login(sql);login(sql,'u2','b');
 let res=await contactsApi(db,'a',new Request('https://member/api/member/contacts'));assert.equal(res.status,200);let data=await res.json();assert.equal(data.contacts.length,1);assert.deepEqual(data.recipients,['a@example.com']);
 const other=sql.prepare("SELECT id FROM sneak_contacts WHERE site_id='b'").get().id;
 assert.equal((await contactsApi(db,'a',new Request('https://member/api/member/contacts?contact='+other))).status,404);
 const payload={site_id:'b',signup_notifications:true,inquiry_notifications:true,weekly_digest:true,digest_day:5,digest_hour:17,timezone:'America/New_York',popup_mode:'after_views',popup_after_views:3};
 const put=()=>new Request('https://member/api/member/contacts',{method:'PUT',body:JSON.stringify(payload)});
 assert.equal((await contactsApi(db,'a',put())).status,404);payload.site_id='a';assert.equal((await contactsApi(db,'a',put(),false)).status,403);
 assert.equal((await contactsApi(db,'a',put())).status,200);payload.timezone='Bogus/Zone';assert.equal((await contactsApi(db,'a',put())).status,400);
 assert.equal((await contactsApi(db,'a',new Request('https://member/api/member/contacts',{method:'PUT',body:'null'}))).status,400);
 }finally{sql.close();}
});
test('weekly scheduling uses customer timezone across DST and does not run early',()=>{
 const settings={timezone:'America/New_York',digest_day:5,digest_hour:17};
 assert.equal(weeklyWindow(new Date('2026-09-25T20:59:00Z'),settings).end,'2026-09-18T21:00:00.000Z');
 const w=weeklyWindow(new Date('2026-11-06T22:01:00Z'),settings);
 assert.equal(w.end,'2026-11-06T22:00:00.000Z');assert.equal(w.start,'2026-10-30T21:00:00.000Z');
});
test('delivery outbox sends once, retries transient failure and respects settings changes',async()=>{
 const {sql,db}=fixture();try{
 sql.exec("UPDATE sneak_contact_settings SET weekly_digest=0");inquiry(sql);login(sql);
 const now=new Date('2026-09-25T15:00:00Z');let sent=[];
 const testEnv={...env,MAILER:{fetch:async(url,opts)=>{sent.push(JSON.parse(opts.body));return Response.json({success:true,providerMessageId:'123'});}}};
 const result=await processOwnerNotifications({db,env:testEnv,now});assert.equal(result.sent,2);assert.ok(sent.every(x=>x.to==='a@example.com'));
 assert.equal((await processOwnerNotifications({db,env:testEnv,now})).sent,0);
 inquiry(sql,'l2');const failure={...env,MAILER:{fetch:async()=>Response.json({success:false,retryable:true,errorCode:'Temporary'})}};
 assert.equal((await processOwnerNotifications({db,env:failure,now})).failed,1);
 assert.equal((await processOwnerNotifications({db,env:testEnv,now})).sent,0);
 sql.exec("UPDATE sneak_contact_settings SET inquiry_notifications=0 WHERE site_id='a'");
 await processOwnerNotifications({db,env:testEnv,now:new Date('2026-09-25T15:06:00Z')});
 assert.equal(sql.prepare("SELECT status FROM sneak_owner_email_deliveries WHERE notification_id='inquiry_l2'").get().status,'cancelled');
 }finally{sql.close();}
});
test('weekly summaries include scoped activity, conceal withheld addresses, escape contact names',async()=>{
 const {sql,db}=fixture();try{inquiry(sql);login(sql);login(sql,'u2','b');
 sql.exec("INSERT INTO sneak_listings(ListingKey,UnparsedAddress,InternetAddressDisplayYN) VALUES('home1','Private address',0)");
 sql.exec("INSERT INTO sneak_consumer_activity_events(id,site_id,user_id,event_type,listing_key,created_at) VALUES('v1','a','u1','listing_view','home1','2026-09-25T11:00:00Z'),('v2','b','u2','listing_view','home1','2026-09-25T11:00:00Z')");
 sql.exec("UPDATE sneak_contacts SET name='<script>bad</script>' WHERE site_id='a'");
 const msg=await renderOwnerNotification(db,{kind:'weekly',period_start:'2026-09-25T00:00:00Z',period_end:'2026-09-26T00:00:00Z'},{id:'a',site_name:'A'});
 assert.match(msg.text,/1 identified contacts/);assert.match(msg.text,/Address withheld/);assert.doesNotMatch(msg.text,/Private address/);assert.doesNotMatch(msg.html,/<script>/);assert.match(msg.html,/&lt;script&gt;/);
 }finally{sql.close();}
});
test('shared mailer rejects unauthenticated and invalid messages; dashboard script parses',async()=>{
 assert.equal((await handleInternalMail(new Request('https://mailer/internal/email',{method:'POST',body:'{}'}),{SNEAK_MAILER_SECRET:'test'})).status,401);
 assert.equal((await handleInternalMail(new Request('https://mailer/internal/email',{method:'POST',headers:{Authorization:'Bearer test'},body:'null'}),{SNEAK_MAILER_SECRET:'test'})).status,400);
 assert.doesNotThrow(()=>new Function(contactsDashboardScript()));
});
test('secure hosted portal enforces entitlement and strips handoff code from top URL',async()=>{
 const {sql,db}=fixture();try{
 const e={DB:db,SNEAK_SIGNING_SECRET:'a-long-enough-test-signing-secret'};
 const res=await handleHostedPortal(new Request('https://idx.example/portal?site=a&signin=1&auth_code=one-use'),e);assert.equal(res.status,200);const html=await res.text();assert.match(html,/history.replaceState/);assert.match(html,/auth_code=one-use/);assert.equal(res.headers.get('Referrer-Policy'),'no-referrer');
 sql.exec("UPDATE sneak_account_entitlements SET status='canceled' WHERE account_id='a'");
 assert.equal((await handleHostedPortal(new Request('https://idx.example/portal?site=a'),e)).status,403);
 }finally{sql.close();}
});

test('member route uses owner role from the verified session to authorize settings writes',async()=>{
 const {default:worker}=await import('../sneak-member/worker.js');const {sha256Hex}=await import('../sneak-member/auth.js');
 const {sql,db}=fixture();try{
 const token='owner-session-token-for-this-test-only-123';const now=new Date().toISOString();
 sql.prepare('INSERT INTO sneak_member_sessions(id,user_id,account_id,token_hash,created_at,expires_at) VALUES(?,?,?,?,?,?)').run('session','a','a',await sha256Hex(token),now,'2099-01-01T00:00:00Z');
 const req=new Request('https://member.example/api/member/contacts',{method:'PUT',headers:{Host:'member.example',Origin:'https://member.example',Cookie:'__Host-sneak_member_session='+token,'Content-Type':'application/json'},body:JSON.stringify({site_id:'a',signup_notifications:true,inquiry_notifications:true,weekly_digest:true,digest_day:5,digest_hour:17,timezone:'America/New_York',popup_mode:'after_views',popup_after_views:3})});
 const res=await worker.fetch(req,{DB:db});assert.equal(res.status,200,await res.text());
 }finally{sql.close();}
});
test('lead endpoint validates fields, stores inquiries and rate limits before creating excess notifications',async()=>{
 const {default:worker}=await import('../SneakIDXWorker.js');const {sql,db}=fixture();try{
 const env={DB:db,SNEAK_SIGNING_SECRET:'a-long-enough-test-signing-secret',SNEAK_ENV:'production'};
 const portal=await handleHostedPortal(new Request('https://idx.example/portal?site=a'),env);
 const html=await portal.text();const src=html.match(/<iframe src="([^"]+)/)[1].replaceAll('&amp;','&');const token=new URL(src).searchParams.get('session');
 const post=body=>worker.fetch(new Request('https://idx.example/idx/v1/lead?site=a',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json','CF-Connecting-IP':'192.0.2.1'},body:JSON.stringify(body)}),env,{});
 assert.equal((await post({name:123,email:'buyer@example.com'})).status,400);
 assert.equal((await post({companyWebsite:'bot.example'})).status,201);
 for(let i=0;i<10;i++)assert.equal((await post({name:'Buyer',email:'buyer@example.com',message:'Tour please'})).status,201);
 assert.equal((await post({name:'Buyer',email:'buyer@example.com'})).status,429);
 assert.equal(sql.prepare('SELECT count(*) n FROM sneak_leads').get().n,10);
 }finally{sql.close();}
});
test('Mailjet validation mode never reports a delivered email',async t=>{
 const {sendTransactionalEmail}=await import('../sneak-shared/email-provider.js');let payload;
 t.mock.method(globalThis,'fetch',async(url,options)=>{payload=JSON.parse(options.body);return Response.json({Messages:[{Status:'success'}]});});
 const result=await sendTransactionalEmail({MAILJET_API_KEY:'test',MAILJET_SECRET_KEY:'test'},{to:'qa@example.com',subject:'Validate configuration',html:'<p>Test</p>',sandbox:true});
 assert.equal(payload.SandboxMode,true);assert.equal(result.validated,true);assert.equal(result.success,false);assert.equal(result.status,'validated');
});

test('passwordless signup, one-use exchange and notification work together on the real schema',async()=>{
 const auth=await import('../sneak-consumer/auth.js');const {sql,db}=fixture();let email;
 try{
 const authEnv={SNEAK_ENV:'production',SNEAK_SERVING_URL:'https://idx.example',CONSUMER_WORKER_URL:'https://consumer.example',SNEAK_MAILER_SECRET:'test',MAILER:{fetch:async(url,opts)=>{email=JSON.parse(opts.body);return Response.json({success:true,providerMessageId:'123'});}}};
 const result=await auth.requestConsumerMagicLink(db,{siteKey:'a',email:'newbuyer@example.com',returnUrl:'https://idx.example/portal?site=a',ipHash:'test-ip',env:authEnv});
 assert.equal(result.success,true);assert.equal(sql.prepare("SELECT count(*) n FROM sneak_contacts").get().n,0);
 const verifyUrl=email.text.match(/https:\/\/consumer\.example\/\S+/)[0];const rawToken=new URL(verifyUrl).searchParams.get('token');
 const verified=await auth.verifyAndConsumeConsumerMagicLink(db,rawToken);assert.equal(verified.siteKey,'a');assert.equal(await auth.verifyAndConsumeConsumerMagicLink(db,rawToken),null);
 const session=await auth.exchangeAuthCodeForSession(db,{code:verified.exchangeCode,siteKey:'a'});assert.ok(session.consumerSession);
 assert.equal(await auth.exchangeAuthCodeForSession(db,{code:verified.exchangeCode,siteKey:'a'}),null);
 assert.equal((await auth.verifyConsumerSession(db,session.consumerSession,'a')).email,'newbuyer@example.com');
 assert.equal(sql.prepare("SELECT count(*) n FROM sneak_owner_notifications WHERE kind='signup'").get().n,1);
 assert.equal(sql.prepare('SELECT count(*) n FROM sneak_contacts').get().n,1);
 }finally{sql.close();}
});

test('consumer health recognizes the configured mailer service without exposing secrets',async()=>{
 const {default:worker}=await import('../sneak-consumer/worker.js');
 const response=await worker.fetch(new Request('https://consumer.example/api/consumer/version'),{MAILER:{},SNEAK_MAILER_SECRET:'private-secret',CONSUMER_AUTH_ENABLED:'true'});
 const body=await response.json();assert.equal(body.emailProviderConfigured,true);assert.equal(body.authEnabled,true);assert.doesNotMatch(JSON.stringify(body),/private-secret/);
});

test('platform BCC applies to direct and relayed mail without changing primary recipients',async t=>{
 const {sendTransactionalEmail}=await import('../sneak-shared/email-provider.js');const messages=[];
 t.mock.method(globalThis,'fetch',async(url,options)=>{messages.push(JSON.parse(options.body).Messages[0]);return Response.json({Messages:[{Status:'success',To:[{MessageID:'123'}]}]});});
 const sendingEnv={MAILJET_API_KEY:'test',MAILJET_SECRET_KEY:'test',EMAIL_BCC:'tech@berealtors.org',SNEAK_MAILER_SECRET:'test-relay'};
 const message={to:'buyer@example.com',subject:'Sign-in link',html:'<p>Sign in</p>'};
 assert.equal((await sendTransactionalEmail(sendingEnv,message)).success,true);
 const relayEnv={SNEAK_MAILER_SECRET:'test-relay',MAILER:{fetch:(url,options)=>handleInternalMail(new Request(url,options),sendingEnv)}};
 assert.equal((await sendTransactionalEmail(relayEnv,message)).success,true);
 for(const payload of messages){assert.deepEqual(payload.To,[{Email:'buyer@example.com'}]);assert.deepEqual(payload.Bcc,[{Email:'tech@berealtors.org'}]);assert.equal(payload.Cc,undefined);}
 await sendTransactionalEmail(sendingEnv,{...message,to:'TECH@berealtors.org'});assert.equal(messages.at(-1).Bcc,undefined);
 await sendTransactionalEmail({...sendingEnv,EMAIL_BCC:''},message);assert.equal(messages.at(-1).Bcc,undefined);
});

test('property inquiries resolve listing details and links, including MLS-ID references',async()=>{
 const {sql,db}=fixture();try{
 inquiry(sql);sql.exec("INSERT INTO sneak_listings(ListingKey,ListingId,UnparsedAddress,City,StateOrProvince,PostalCode,ListPrice,BedroomsTotal,BathroomsTotalInteger,LivingArea,StandardStatus) VALUES('internal-key','226028872','15054 Cuberra LN','BONITA SPRINGS','FL','34135',949900,3,3,2486,'Active')");
 for(const key of ['internal-key','226028872']){
 sql.prepare("UPDATE sneak_leads SET listing_key=?,lead_type='contact_agent',message='<script>test</script>' WHERE id='l1'").run(key);
 const mail=await renderOwnerNotification(db,{kind:'inquiry',reference_id:'l1'},{id:'a',site_name:'Ursula'});
 assert.match(mail.text,/15054 Cuberra LN, BONITA SPRINGS, FL 34135/);assert.match(mail.text,/MLS #: 226028872/);assert.match(mail.text,/Price: \$949,900/);assert.match(mail.text,/3 beds · 3 baths · 2,486 sqft/);assert.match(mail.text,/Request: Speak with an agent/);
 assert.match(mail.html,/href="https:\/\/sneak-idx-worker[^\"]+site=a&amp;ccor_listing=internal-key"/);assert.doesNotMatch(mail.html,/<script>/);
 }
 sql.exec("UPDATE sneak_listings SET InternetAddressDisplayYN=0");let mail=await renderOwnerNotification(db,{kind:'inquiry',reference_id:'l1'},{id:'a'});assert.doesNotMatch(mail.text,/Cuberra/);assert.match(mail.text,/Address Undisclosed/);
 sql.exec("UPDATE sneak_listings SET InternetEntireListingDisplayYN=0");mail=await renderOwnerNotification(db,{kind:'inquiry',reference_id:'l1'},{id:'a'});assert.match(mail.text,/no longer available/);assert.doesNotMatch(mail.text,/949,900|Cuberra|View property:/);
 sql.exec("UPDATE sneak_listings SET InternetEntireListingDisplayYN=1; UPDATE sneak_sites SET scope_type='agent',scope_value='another-agent' WHERE id='a'");mail=await renderOwnerNotification(db,{kind:'inquiry',reference_id:'l1'},{id:'a'});assert.doesNotMatch(mail.text,/949,900|View property:/);
 sql.exec('DELETE FROM sneak_listings');mail=await renderOwnerNotification(db,{kind:'inquiry',reference_id:'l1'},{id:'a'});assert.match(mail.text,/Listing reference: 226028872/);
 }finally{sql.close();}
});
test('hosted property email links carry the requested listing into the search frame',async()=>{
 const {sql,db}=fixture();try{const res=await handleHostedPortal(new Request('https://idx.example/portal?site=a&ccor_listing=internal-key'),{DB:db,SNEAK_SIGNING_SECRET:'a-long-enough-test-signing-secret'});assert.equal(res.status,200);assert.match(await res.text(),/ccor_listing=internal-key/);}finally{sql.close();}
});

test('member email previews do not consume tokens; confirmed sign-in redirects and prevents replay',async()=>{
 const {default:worker}=await import('../sneak-member/worker.js');const auth=await import('../sneak-member/auth.js');const {sql,db}=fixture();
 try{
 const first=await auth.createMagicLinkRecord(db,'a','login',900);const second=await auth.createMagicLinkRecord(db,'a','login',900);
 assert.equal(sql.prepare('SELECT count(*) n FROM sneak_member_magic_links WHERE used_at IS NULL').get().n,2);
 for(let i=0;i<2;i++){const preview=await worker.fetch(new Request('https://member.example/api/member/auth/verify?token='+first),{DB:db});assert.equal(preview.status,200);assert.equal(preview.headers.get('Set-Cookie'),null);assert.match(await preview.text(),/method="post"/);}
 assert.equal(sql.prepare('SELECT count(*) n FROM sneak_member_magic_links WHERE used_at IS NOT NULL').get().n,0);
 const request=(token,origin='https://member.example')=>new Request('https://member.example/api/member/auth/verify',{method:'POST',headers:{Host:'member.example',Origin:origin,'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({token})});
 assert.equal((await worker.fetch(request(first,'https://attacker.example'),{DB:db})).status,403);
 const signed=await worker.fetch(request(first),{DB:db});assert.equal(signed.status,303);assert.equal(signed.headers.get('Location'),'/');assert.match(signed.headers.get('Set-Cookie'),/__Host-sneak_member_session=.*HttpOnly; Secure/);
 const replay=await worker.fetch(request(first),{DB:db});assert.equal(replay.status,401);assert.match(await replay.text(),/request another email/);
 sql.prepare('UPDATE sneak_member_magic_links SET expires_at=? WHERE token_hash=?').run('2000-01-01T00:00:00Z',await auth.sha256Hex(second));
 assert.equal((await worker.fetch(request(second),{DB:db})).status,401);
 }finally{sql.close();}
});
test('member rate limits count only recent attempts, and email failures are not reported as sent',async()=>{
 const auth=await import('../sneak-member/auth.js');const {sql,db}=fixture();try{
 const hash=await auth.sha256Hex('a@example.com');const old=new Date(Date.now()-3600000).toISOString();
 for(let i=0;i<12;i++)sql.prepare('INSERT INTO sneak_member_login_attempts(id,ip_hash,email_hash,attempted_at) VALUES(?,?,?,?)').run('old'+i,'ip',hash,old);
 const cfg={SNEAK_MAILER_SECRET:'test',MAILER:{fetch:async()=>Response.json({success:true,providerMessageId:'123'})}};
 assert.equal((await auth.requestPublicMagicLink(db,'a@example.com','ip',cfg)).success,true);
 const failed=await auth.requestPublicMagicLink(db,'a@example.com','ip',{});assert.equal(failed.success,false);assert.match(failed.message,/temporarily unavailable/);
 for(let i=0;i<10;i++)sql.prepare('INSERT INTO sneak_member_login_attempts(id,ip_hash,email_hash,attempted_at) VALUES(?,?,?,?)').run('new'+i,'ip',hash,new Date().toISOString());
 const limited=await auth.requestPublicMagicLink(db,'a@example.com','ip',cfg);assert.equal(limited.rateLimited,true);
 }finally{sql.close();}
});
test('authentication mail keeps direct links while retaining the monitoring BCC',async t=>{
 const {sendTransactionalEmail}=await import('../sneak-shared/email-provider.js');let message;
 t.mock.method(globalThis,'fetch',async(url,options)=>{message=JSON.parse(options.body).Messages[0];return Response.json({Messages:[{Status:'success',To:[{MessageID:'123'}]}]});});
 await sendTransactionalEmail({MAILJET_API_KEY:'test',MAILJET_SECRET_KEY:'test',EMAIL_BCC:'tech@berealtors.org'},{to:'buyer@example.com',subject:'Sign in',html:'<a>Sign in</a>',customId:'SNEAK-IDX-MEMBER'});
 assert.equal(message.TrackClicks,'disabled');assert.equal(message.TrackOpens,'disabled');assert.deepEqual(message.Bcc,[{Email:'tech@berealtors.org'}]);
});
