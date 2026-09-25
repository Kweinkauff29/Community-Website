import { WorkerEntrypoint } from 'cloudflare:workers';
import { processOwnerNotifications } from '../sneak-shared/contacts.js';
export { default } from './worker.js';
/** Accessible only through a named service binding, never through public HTTP. */
export class OwnerNotifier extends WorkerEntrypoint {
 async dispatch(notificationId) {
  if (typeof notificationId !== 'string' || !/^(inquiry|signup)_[a-zA-Z0-9_-]{1,150}$/.test(notificationId)) return {invalid:true};
  return processOwnerNotifications({db:this.env.DB,env:this.env,notificationId});
 }
}
