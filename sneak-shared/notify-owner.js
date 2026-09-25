/** Dispatch an existing outbox event; the scheduled worker retains retry ownership. */
export async function notifyOwner(env, ctx, notificationId) {
 if (!env.OWNER_NOTIFIER) return;
 const send=Promise.resolve().then(()=>env.OWNER_NOTIFIER.dispatch(notificationId)).catch(()=>{
  console.error(JSON.stringify({event:'owner_notification_deferred',reason:'ImmediateDispatchFailed'}));
 });
 if (ctx?.waitUntil) ctx.waitUntil(send); else await send;
}
