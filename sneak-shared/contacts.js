import { isAccountEntitled } from './entitlement.js';
import { sendTransactionalEmail } from './email-provider.js';

const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const reply = (data,status=200) => new Response(JSON.stringify(data),{status,headers:{'Content-Type':'application/json','Cache-Control':'no-store'}});
const rows = async statement => (await statement.all()).results || [];
const changes = result => result?.meta?.changes ?? result?.changes ?? 0;

export async function contactsApi(db,accountId,request,canEdit=true) {
 const url=new URL(request.url);
 if(request.method==='PUT') {
  if(!canEdit)return reply({error:'Forbidden'},403);
  let b;try{b=await request.json();}catch{return reply({error:'InvalidJSON'},400);}
  if(!b || typeof b.site_id!=='string' || typeof b.timezone!=='string')return reply({error:'InvalidSettings'},400);
  const site=await db.prepare('SELECT id FROM sneak_sites WHERE id=? AND account_id=?').bind(b.site_id,accountId).first();
  if(!site)return reply({error:'SiteNotFound'},404);
  if(![b.signup_notifications,b.inquiry_notifications,b.weekly_digest].every(x=>typeof x==='boolean') || !Number.isInteger(b.digest_day)||b.digest_day<0||b.digest_day>6||!Number.isInteger(b.digest_hour)||b.digest_hour<0||b.digest_hour>23||!['off','optional','after_views'].includes(b.popup_mode)||!Number.isInteger(b.popup_after_views)||b.popup_after_views<1||b.popup_after_views>50)return reply({error:'InvalidSettings'},400);
  try{new Intl.DateTimeFormat('en-US',{timeZone:b.timezone}).format();}catch{return reply({error:'InvalidTimezone'},400);}
  await db.prepare(`UPDATE sneak_contact_settings SET signup_notifications=?,inquiry_notifications=?,weekly_digest=?,digest_day=?,digest_hour=?,timezone=?,popup_mode=?,popup_after_views=?,updated_at=CURRENT_TIMESTAMP WHERE site_id=?`).bind(+b.signup_notifications,+b.inquiry_notifications,+b.weekly_digest,b.digest_day,b.digest_hour,b.timezone,b.popup_mode,b.popup_after_views,b.site_id).run();
  return reply({success:true});
 }
 const contactId=url.searchParams.get('contact');
 if(contactId){
  const contact=await db.prepare('SELECT c.* FROM sneak_contacts c JOIN sneak_sites s ON s.id=c.site_id WHERE c.id=? AND s.account_id=?').bind(contactId,accountId).first();
  if(!contact)return reply({error:'ContactNotFound'},404);
  const inquiries=await rows(db.prepare('SELECT id,listing_key,lead_type,name,email,phone,message,created_at FROM sneak_leads WHERE site_id=? AND lower(trim(email))=? ORDER BY created_at DESC LIMIT 100').bind(contact.site_id,contact.email));
  const activity=await rows(db.prepare(`SELECT e.event_type,e.listing_key,e.created_at,CASE WHEN l.InternetAddressDisplayYN=0 THEN 'Address withheld' ELSE l.UnparsedAddress END AS address FROM sneak_consumer_activity_events e LEFT JOIN sneak_listings l ON l.ListingKey=e.listing_key WHERE e.site_id=? AND e.user_id=? ORDER BY e.created_at DESC LIMIT 100`).bind(contact.site_id,contact.consumer_id||''));
  const logins=await rows(db.prepare('SELECT event_type,created_at FROM sneak_contact_events WHERE site_id=? AND contact_id=? ORDER BY created_at DESC LIMIT 100').bind(contact.site_id,contactId));
  return reply({contact,inquiries,activity,logins});
 }
 const search=(url.searchParams.get('search')||'').trim().slice(0,100);
 const page=Math.max(1,parseInt(url.searchParams.get('page'),10)||1);
 const like='%'+search+'%';
 const total=await db.prepare(`SELECT COUNT(*) total FROM sneak_contacts c JOIN sneak_sites s ON s.id=c.site_id WHERE s.account_id=? AND (c.email LIKE ? OR c.name LIKE ? OR c.phone LIKE ?)`).bind(accountId,like,like,like).first();
 const contacts=await rows(db.prepare(`SELECT c.*,s.site_name,u.last_login_at,u.status AS login_status,(SELECT count(*) FROM sneak_leads l WHERE l.site_id=c.site_id AND lower(trim(l.email))=c.email) inquiry_count FROM sneak_contacts c JOIN sneak_sites s ON s.id=c.site_id LEFT JOIN sneak_consumer_users u ON u.id=c.consumer_id AND u.site_id=c.site_id WHERE s.account_id=? AND (c.email LIKE ? OR c.name LIKE ? OR c.phone LIKE ?) ORDER BY datetime(c.last_activity_at) DESC LIMIT 50 OFFSET ?`).bind(accountId,like,like,like,(page-1)*50));
 const settings=await rows(db.prepare(`SELECT p.*,s.site_name,s.site_key FROM sneak_contact_settings p JOIN sneak_sites s ON s.id=p.site_id WHERE s.account_id=?`).bind(accountId));
 const recipients=await rows(db.prepare("SELECT email FROM sneak_member_users WHERE account_id=? AND role IN ('owner','admin') AND status IN ('active','invited')").bind(accountId));
 const deliveries=await rows(db.prepare(`SELECT d.recipient,d.status,d.attempts,d.last_error,d.sent_at,n.kind,n.created_at FROM sneak_owner_email_deliveries d JOIN sneak_owner_notifications n ON n.id=d.notification_id JOIN sneak_sites s ON s.id=n.site_id WHERE s.account_id=? ORDER BY n.created_at DESC LIMIT 25`).bind(accountId));
 return reply({contacts,total:total?.total||0,page,totalPages:Math.ceil((total?.total||0)/50),settings,recipients:recipients.map(x=>x.email),deliveries});
}

function localParts(date,timeZone){return Object.fromEntries(new Intl.DateTimeFormat('en-CA',{timeZone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).formatToParts(date).filter(p=>p.type!=='literal').map(p=>[p.type,Number(p.value)]));}
function zonedTime(year,month,day,hour,timeZone){const target=Date.UTC(year,month-1,day,hour);let guess=target;for(let i=0;i<4;i++){const p=localParts(new Date(guess),timeZone);const observed=Date.UTC(p.year,p.month-1,p.day,p.hour,p.minute,p.second);guess+=target-observed;}return new Date(guess);}
export function weeklyWindow(now,settings){
 const p=localParts(now,settings.timezone);const date=new Date(Date.UTC(p.year,p.month-1,p.day));
 let delta=(date.getUTCDay()-settings.digest_day+7)%7;
 if(delta===0&&p.hour<settings.digest_hour)delta=7;
 date.setUTCDate(date.getUTCDate()-delta);
 const key=date.toISOString().slice(0,10);
 const end=zonedTime(date.getUTCFullYear(),date.getUTCMonth()+1,date.getUTCDate(),settings.digest_hour,settings.timezone);
 date.setUTCDate(date.getUTCDate()-7);
 const start=zonedTime(date.getUTCFullYear(),date.getUTCMonth()+1,date.getUTCDate(),settings.digest_hour,settings.timezone);
 return {key,start:start.toISOString(),end:end.toISOString()};
}

export async function weeklyActivity(db,siteId,start,end){
 const contacts=await rows(db.prepare(`SELECT c.id,c.name,c.email,c.consumer_id,
 (SELECT count(*) FROM sneak_contact_events e WHERE e.contact_id=c.id AND e.site_id=c.site_id AND e.event_type='login' AND datetime(e.created_at)>=datetime(?) AND datetime(e.created_at)<datetime(?)) logins,
 (SELECT count(*) FROM sneak_leads l WHERE l.site_id=c.site_id AND lower(trim(l.email))=c.email AND datetime(l.created_at)>=datetime(?) AND datetime(l.created_at)<datetime(?)) inquiries
 FROM sneak_contacts c WHERE c.site_id=? AND (
 EXISTS(SELECT 1 FROM sneak_contact_events e WHERE e.contact_id=c.id AND e.site_id=c.site_id AND datetime(e.created_at)>=datetime(?) AND datetime(e.created_at)<datetime(?)) OR
 EXISTS(SELECT 1 FROM sneak_consumer_activity_events e WHERE e.user_id=c.consumer_id AND e.site_id=c.site_id AND datetime(e.created_at)>=datetime(?) AND datetime(e.created_at)<datetime(?))) ORDER BY c.email`).bind(start,end,start,end,siteId,start,end,start,end));
 const activity=await rows(db.prepare(`SELECT e.user_id,e.event_type,e.listing_key,COUNT(*) AS count,CASE WHEN l.InternetAddressDisplayYN=0 THEN 'Address withheld' ELSE l.UnparsedAddress END AS address FROM sneak_consumer_activity_events e LEFT JOIN sneak_listings l ON l.ListingKey=e.listing_key WHERE e.site_id=? AND datetime(e.created_at)>=datetime(?) AND datetime(e.created_at)<datetime(?) GROUP BY e.user_id,e.event_type,e.listing_key`).bind(siteId,start,end));
 return contacts.map(c=>({...c,activity:activity.filter(e=>e.user_id===c.consumer_id)}));
}

export async function renderOwnerNotification(db,notification,site){
 const heading=site.site_name||'Your IDX website';
 let subject,lines;
 if(notification.kind==='weekly'){
  const contacts=await weeklyActivity(db,site.id,notification.period_start,notification.period_end);
  subject=`Weekly IDX activity — ${heading}`;
  lines=[`${heading}: ${notification.period_start} to ${notification.period_end}`,`${contacts.length} identified contacts used your IDX site.`,...contacts.flatMap(c=>[`${c.name?c.name+' — ':''}${c.email}: ${c.logins} sign-ins, ${c.inquiries} inquiries`,...c.activity.map(a=>`  ${a.event_type.replaceAll('_',' ')}: ${a.address||a.listing_key||'Saved search/account activity'} (${a.count})`)])];
  if(!contacts.length)lines.push('No identified contact activity this week.');
  lines.push('Anonymous browsing is not attributed to a person. Activity is recorded for signed-in visitors.');
 }else if(notification.kind==='signup'){
  const u=await db.prepare('SELECT email FROM sneak_consumer_users WHERE id=? AND site_id=? AND status=\'active\'').bind(notification.reference_id,site.id).first();
  if(!u)return null;
  subject=`New verified IDX contact — ${heading}`;lines=[`${u.email} signed in to ${heading} for the first time.`];
 }else{
  const l=await db.prepare('SELECT * FROM sneak_leads WHERE id=? AND site_id=?').bind(notification.reference_id,site.id).first();if(!l)return null;
  subject=`New property inquiry — ${heading}`;lines=[`Name: ${l.name}`,`Email: ${l.email}`,`Phone: ${l.phone||'Not supplied'}`,`Request: ${l.lead_type}`,`Listing: ${l.listing_key||'General contact'}`,`Message: ${l.message||''}`];
 }
 const dashboard='https://sneak-idx-member.bonitaspringsrealtors.workers.dev/';
 lines.push('Open your Contacts & Email dashboard: '+dashboard);
 return {subject,text:lines.join('\n'),html:`<h2>${escape(subject)}</h2>${lines.map(l=>`<p>${escape(l)}</p>`).join('')}<p><a href="${dashboard}">View contacts and activity</a></p>`};
}

export async function processOwnerNotifications({db,env,now=new Date(),dryRun=false}){
 if(env.OWNER_NOTIFICATIONS_ENABLED!=='true')return {disabled:true};
 const nowIso=now.toISOString();
 if(!dryRun)await db.prepare('DELETE FROM sneak_lead_rate_limits WHERE expires_at<?').bind(nowIso).run();
 const sites=(await rows(db.prepare(`SELECT s.id,s.site_name,p.*,a.status AS account_status,e.status AS entitlement_status,e.grace_until,e.expires_at FROM sneak_sites s JOIN sneak_accounts a ON a.id=s.account_id LEFT JOIN sneak_account_entitlements e ON e.account_id=a.id JOIN sneak_contact_settings p ON p.site_id=s.id WHERE s.status='active' AND a.status='active'`))).filter(s=>isAccountEntitled(s.account_status,s.entitlement_status,s.grace_until,now,s.expires_at));
 for(const site of sites){
  if(!site.weekly_digest)continue;
  const period=weeklyWindow(now,site);
  if(new Date(period.end)<new Date(site.created_at.endsWith('Z')?site.created_at:site.created_at.replace(' ','T')+'Z'))continue;
  if(!dryRun)await db.prepare(`INSERT OR IGNORE INTO sneak_owner_notifications(id,site_id,kind,period_start,period_end) VALUES(?,?,'weekly',?,?)`).bind('weekly_'+site.id+'_'+period.key,site.id,period.start,period.end).run();
 }
 const pending=await rows(db.prepare(`SELECT n.* FROM sneak_owner_notifications n JOIN sneak_sites s ON s.id=n.site_id JOIN sneak_accounts a ON a.id=s.account_id WHERE n.processed_at IS NULL AND s.status='active' AND a.status='active' ORDER BY n.created_at LIMIT 100`));
 for(const n of pending){
  const config=sites.find(s=>s.id===n.site_id);if(!config)continue;
  const enabled=n.kind==='signup'?config.signup_notifications:n.kind==='inquiry'?config.inquiry_notifications:config.weekly_digest;
  const recipients=enabled?await rows(db.prepare(`SELECT DISTINCT lower(u.email) email FROM sneak_member_users u JOIN sneak_sites s ON s.account_id=u.account_id WHERE s.id=? AND u.status IN ('active','invited') AND u.role IN ('owner','admin')`).bind(n.site_id)):[];
  if(enabled&&!recipients.length)continue;
  if(!dryRun){
   await db.batch([...recipients.map(r=>db.prepare('INSERT OR IGNORE INTO sneak_owner_email_deliveries(id,notification_id,recipient) VALUES(?,?,?)').bind(crypto.randomUUID(),n.id,r.email)),db.prepare('UPDATE sneak_owner_notifications SET processed_at=? WHERE id=?').bind(nowIso,n.id)]);
  }
 }
 const due=await rows(db.prepare(`SELECT d.*,n.site_id,n.kind,n.reference_id,n.period_start,n.period_end FROM sneak_owner_email_deliveries d JOIN sneak_owner_notifications n ON n.id=d.notification_id JOIN sneak_sites s ON s.id=n.site_id JOIN sneak_accounts a ON a.id=s.account_id WHERE s.status='active' AND a.status='active' AND d.attempts<5 AND ((d.status IN ('pending','failed') AND (d.next_attempt_at IS NULL OR d.next_attempt_at<=?)) OR (d.status='sending' AND d.claimed_at<?)) ORDER BY n.created_at LIMIT 25`).bind(nowIso,new Date(now-15*60000).toISOString()));
 const stats={pending:pending.length,due:due.length,sent:0,failed:0,dryRun};
 for(const d of due){
  const site=sites.find(s=>s.id===d.site_id);if(!site)continue;
  const enabled=d.kind==='signup'?site.signup_notifications:d.kind==='inquiry'?site.inquiry_notifications:site.weekly_digest;
  const recipient=await db.prepare(`SELECT u.id FROM sneak_member_users u JOIN sneak_sites s ON s.account_id=u.account_id WHERE s.id=? AND lower(u.email)=? AND u.status IN ('active','invited') AND u.role IN ('owner','admin')`).bind(d.site_id,d.recipient).first();
  const content=enabled&&recipient?await renderOwnerNotification(db,d,site):null;
  if(!content){if(!dryRun)await db.prepare("UPDATE sneak_owner_email_deliveries SET status='cancelled',last_error='NotificationNoLongerApplicable' WHERE id=?").bind(d.id).run();continue;}
  if(dryRun)continue;
  const claim=await db.prepare(`UPDATE sneak_owner_email_deliveries SET status='sending',attempts=attempts+1,claimed_at=? WHERE id=? AND attempts=? AND (status IN ('pending','failed') OR (status='sending' AND claimed_at<?))`).bind(nowIso,d.id,d.attempts,new Date(now-15*60000).toISOString()).run();
  if(!changes(claim))continue;
  const result=await sendTransactionalEmail(env,{to:d.recipient,...content,customId:d.id});
  await db.prepare(`UPDATE sneak_owner_email_deliveries SET status=?,sent_at=?,provider_message_id=?,last_error=?,next_attempt_at=?,attempts=? WHERE id=?`).bind(result.success?'sent':'failed',result.success?nowIso:null,result.providerMessageId||null,result.errorCode||null,new Date(now.getTime()+Math.min(360,5*2**d.attempts)*60000).toISOString(),result.success||result.retryable?d.attempts+1:5,d.id).run();
  stats[result.success?'sent':'failed']++;
 }
 return stats;
}
