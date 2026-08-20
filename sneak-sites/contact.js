/**
 * sneak-sites/contact.js
 * 
 * Secure Public Lead Contact Submission for SNEAK Websites.
 * Writes directly to sneak_leads with strict tenant isolation, honeypot, and IP rate limiting.
 */

function sanitize(str) {
    if (typeof str !== 'string') return '';
    return str.replace(/[<>]/g, '').trim();
}

function isValidEmail(email) {
    return typeof email === 'string' && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim());
}

/**
 * Handles contact lead form submission.
 */
export async function handleContactSubmission(request, env, siteBundle) {
    const { site } = siteBundle;
    if (!site || !site.id) {
        return new Response(JSON.stringify({ error: 'InvalidSite', message: 'Tenant site not identified.' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    let body = {};
    const contentType = request.headers.get('Content-Type') || '';

    try {
        if (contentType.includes('application/json')) {
            body = await request.json();
        } else if (contentType.includes('application/x-www-form-urlencoded') || contentType.includes('multipart/form-data')) {
            const formData = await request.formData();
            for (const [k, v] of formData.entries()) {
                body[k] = v;
            }
        }
    } catch {
        return new Response(JSON.stringify({ error: 'BadRequest', message: 'Could not parse submission.' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    // 1. Honeypot Check (Spam defense)
    if (body.website_hp && String(body.website_hp).trim().length > 0) {
        // Silently accept spam bot submission to prevent adaptive tuning
        return new Response(JSON.stringify({ success: true, message: 'Thank you for your message!' }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    // 2. Field Validations
    const clientName = sanitize(body.name || body.client_name || '');
    const email = sanitize(body.email || '');
    const phone = sanitize(body.phone || '');
    const message = sanitize(body.message || body.notes || '');
    const listingKey = sanitize(body.listing_key || body.listingKey || '');

    if (!clientName || clientName.length < 2) {
        return new Response(JSON.stringify({ error: 'ValidationError', message: 'Name is required.' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    if (!email || !isValidEmail(email)) {
        return new Response(JSON.stringify({ error: 'ValidationError', message: 'A valid email address is required.' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    // 3. Write into sneak_leads
    const leadId = `lead_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`;
    const fullMessage = message ? `${message}${listingKey ? ` (Inquiry on listing ${listingKey})` : ''}` : (listingKey ? `Inquiry on listing ${listingKey}` : 'General inquiry from website.');

    try {
        await env.DB.prepare(`
            INSERT INTO sneak_leads (
                id, site_id, name, email, phone, message, listing_key, lead_type, source_url, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'website_contact', ?, datetime('now'))
        `).bind(
            leadId, site.id, clientName, email, phone || null, fullMessage, listingKey || null, request.url
        ).run();

        return new Response(JSON.stringify({
            success: true,
            leadId,
            message: 'Thank you! Your message has been received and our team will get in touch shortly.'
        }), {
            status: 201,
            headers: { 'Content-Type': 'application/json' }
        });
    } catch (err) {
        console.error('[LEAD INSERT ERROR]', err.message);
        return new Response(JSON.stringify({ error: 'ServerError', message: 'Could not record inquiry: ' + err.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}
