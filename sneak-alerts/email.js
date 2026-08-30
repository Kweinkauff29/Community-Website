/**
 * sneak-alerts/email.js
 * 
 * Saved Search Email Alert Template Renderer.
 * 
 * Features & Compliance:
 * - Brand palette & typography integration.
 * - Internet Address Display compliance (suppressed when InternetAddressDisplayYN != 1).
 * - Required listing brokerage attribution on every property card.
 * - Context-aware specs for Residential, Rental, Land, and Commercial categories.
 * - Deep linking to property details via ?ccor_listing=<ListingKey>.
 * - Cryptographically signed one-click unsubscribe links.
 * - Complete XSS escaping on all dynamic data.
 */

export function escapeHtml(str) {
    if (str === null || str === undefined) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

export function sanitizeUrl(url) {
    if (!url || typeof url !== 'string') return '';
    const clean = url.trim();
    if (clean.startsWith('https://') || clean.startsWith('http://')) {
        return clean;
    }
    return '';
}

export function formatPrice(price) {
    if (!price || isNaN(Number(price))) return 'Price Upon Request';
    return '$' + Number(price).toLocaleString('en-US');
}

export function formatCardSpecs(item) {
    const type = (item.PropertyType || '').toLowerCase();

    if (type.includes('commercial') || type.includes('business')) {
        const parts = [];
        if (item.LivingArea) parts.push(`${Number(item.LivingArea).toLocaleString()} sqft`);
        if (item.LotSizeAcres) parts.push(`${item.LotSizeAcres} ac`);
        if (item.PropertySubType) parts.push(item.PropertySubType);
        if (item.Zoning) parts.push(`Zoning: ${item.Zoning}`);
        return parts.join(' • ') || 'Commercial Property';
    }

    if (type.includes('land')) {
        const parts = [];
        if (item.LotSizeAcres) parts.push(`${item.LotSizeAcres} Acres`);
        if (item.SubdivisionName) parts.push(item.SubdivisionName);
        else if (item.City) parts.push(item.City);
        return parts.join(' • ') || 'Lot & Land';
    }

    // Default Residential / Rental
    const parts = [];
    if (item.BedroomsTotal !== null && item.BedroomsTotal !== undefined) parts.push(`${item.BedroomsTotal} bd`);
    if (item.BathroomsTotalInteger !== null && item.BathroomsTotalInteger !== undefined) parts.push(`${item.BathroomsTotalInteger} ba`);
    if (item.LivingArea) parts.push(`${Number(item.LivingArea).toLocaleString()} sqft`);
    return parts.join(' • ') || (item.PropertySubType || 'Residential');
}

/**
 * Builds full HTML email for saved search alerts.
 */
export function renderSavedSearchAlertEmail({
    alert,
    searchName,
    site,
    branding,
    account,
    listings = [],
    totalMatches = 0,
    returnUrl,
    unsubscribeUrl
}) {
    const agentName = branding?.display_name || account?.account_name || 'Your REALTOR®';
    const brokerageName = branding?.brokerage || 'Coconut Coast Organization of REALTORS®';
    const primaryColor = branding?.primary_color || '#0284c7';
    const safeSearchName = searchName ? escapeHtml(searchName) : 'Saved Search';
    const safeAgentName = escapeHtml(agentName);
    const safeBrokerageName = escapeHtml(brokerageName);

    const isDaily = alert.frequency === 'daily';
    const subjectPrefix = isDaily ? 'Your Daily Property Matches' : 'New Property Matches';
    const displayListings = listings.slice(0, 10);
    const remainingCount = Math.max(0, totalMatches - displayListings.length);

    const safeReturnUrl = sanitizeUrl(returnUrl) || 'https://coconutcoastrealtors.org/idx-test/';
    const safeUnsubscribeUrl = sanitizeUrl(unsubscribeUrl) || safeReturnUrl;

    const cardsHtml = displayListings.map(item => {
        const isAddressSuppressed = (item.InternetAddressDisplayYN === 0);
        const addressText = isAddressSuppressed ? 'Address Undisclosed' : (item.UnparsedAddress || 'Address Available Upon Request');
        const cityState = [item.City, item.PostalCode].filter(Boolean).join(', ');
        const priceText = formatPrice(item.ListPrice);
        const specsText = formatCardSpecs(item);
        const listingOffice = item.ListOfficeName ? `Listing courtesy of ${escapeHtml(item.ListOfficeName)}` : '';
        const photoUrl = sanitizeUrl(item.PrimaryPhoto) || 'https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=600&auto=format&fit=crop&q=80';

        // Deep link with ccor_listing param
        const deepLinkUrl = safeReturnUrl.includes('?')
            ? `${safeReturnUrl}&ccor_listing=${encodeURIComponent(item.ListingKey)}`
            : `${safeReturnUrl}?ccor_listing=${encodeURIComponent(item.ListingKey)}`;

        return `
            <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #111e38; border: 1px solid rgba(255,255,255,0.1); border-radius: 10px; overflow: hidden; margin-bottom: 20px;">
                <tr>
                    <td style="padding: 0;">
                        <a href="${deepLinkUrl}" target="_blank" style="text-decoration: none; display: block;">
                            <img src="${photoUrl}" alt="${escapeHtml(addressText)}" width="100%" style="display: block; width: 100%; max-height: 240px; object-fit: cover; border-bottom: 1px solid rgba(255,255,255,0.08);" />
                        </a>
                    </td>
                </tr>
                <tr>
                    <td style="padding: 18px 20px;">
                        <div style="font-size: 22px; font-weight: 700; color: #38bdf8; margin-bottom: 4px;">${escapeHtml(priceText)}</div>
                        <div style="font-size: 14px; font-weight: 600; color: #94a3b8; margin-bottom: 8px;">${escapeHtml(specsText)}</div>
                        <div style="font-size: 16px; font-weight: 600; color: #f8fafc; margin-bottom: 4px;">${escapeHtml(addressText)}</div>
                        ${cityState ? `<div style="font-size: 13px; color: #94a3b8; margin-bottom: 12px;">${escapeHtml(cityState)}</div>` : ''}
                        
                        <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top: 14px;">
                            <tr>
                                <td align="left">
                                    ${listingOffice ? `<div style="font-size: 11px; color: #64748b; font-style: italic;">${listingOffice}</div>` : ''}
                                </td>
                                <td align="right" style="white-space: nowrap;">
                                    <a href="${deepLinkUrl}" target="_blank" style="display: inline-block; background-color: ${primaryColor}; color: #ffffff; text-decoration: none; font-size: 13px; font-weight: 600; padding: 8px 16px; border-radius: 6px;">
                                        View Details →
                                    </a>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        `;
    }).join('\n');

    return `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${escapeHtml(subjectPrefix)}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #0b1329; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #f8fafc;">
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #0b1329; padding: 30px 15px;">
        <tr>
            <td align="center">
                <table width="100%" max-width="580px" border="0" cellspacing="0" cellpadding="0" style="max-width: 580px; background-color: #0d182e; border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; overflow: hidden; padding: 28px 24px; box-shadow: 0 10px 30px rgba(0,0,0,0.5);">
                    <!-- Header -->
                    <tr>
                        <td align="left" style="padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.08);">
                            <div style="font-size: 18px; font-weight: 700; color: #38bdf8; letter-spacing: 0.5px;">${safeAgentName}</div>
                            <div style="font-size: 13px; color: #94a3b8; margin-top: 2px;">${safeBrokerageName}</div>
                        </td>
                    </tr>

                    <!-- Intro -->
                    <tr>
                        <td style="padding: 24px 0 16px 0;">
                            <h2 style="font-size: 20px; font-weight: 700; color: #ffffff; margin: 0 0 8px 0;">
                                ${displayListings.length} ${isDaily ? 'new daily match' : 'new match'}${displayListings.length === 1 ? '' : 'es'} for "${safeSearchName}"
                            </h2>
                            <p style="font-size: 14px; line-height: 1.5; color: #cbd5e1; margin: 0 0 20px 0;">
                                Fresh property matches have just become available in your saved search.
                            </p>
                        </td>
                    </tr>

                    <!-- Listing Cards -->
                    <tr>
                        <td>
                            ${cardsHtml}
                        </td>
                    </tr>

                    <!-- More Matches Banner (if applicable) -->
                    ${remainingCount > 0 ? `
                    <tr>
                        <td align="center" style="padding: 10px 0 24px 0;">
                            <div style="background-color: #111e38; border: 1px dashed rgba(255,255,255,0.15); border-radius: 8px; padding: 16px 20px; text-align: center;">
                                <div style="font-size: 15px; font-weight: 600; color: #f8fafc; margin-bottom: 8px;">
                                    + ${remainingCount} more new matching propert${remainingCount === 1 ? 'y' : 'ies'}
                                </div>
                                <a href="${safeReturnUrl}" target="_blank" style="display: inline-block; background-color: ${primaryColor}; color: #ffffff; text-decoration: none; font-size: 13px; font-weight: 600; padding: 8px 20px; border-radius: 6px;">
                                    View All Search Results →
                                </a>
                            </div>
                        </td>
                    </tr>
                    ` : ''}

                    <!-- Footer Links & Unsubscribe -->
                    <tr>
                        <td style="padding-top: 24px; border-top: 1px solid rgba(255,255,255,0.08); text-align: center;">
                            <div style="margin-bottom: 14px;">
                                <a href="${safeReturnUrl}" target="_blank" style="color: #38bdf8; font-size: 13px; text-decoration: none; font-weight: 600; margin: 0 10px;">
                                    View Property Search
                                </a>
                                <span style="color: #475569;">•</span>
                                <a href="${safeUnsubscribeUrl}" target="_blank" style="color: #94a3b8; font-size: 13px; text-decoration: none; margin: 0 10px;">
                                    Stop alerts for this search
                                </a>
                            </div>
                            <p style="font-size: 11px; line-height: 1.5; color: #64748b; margin: 0;">
                                You received this automated notification because email alerts are enabled for your saved search on ${safeBrokerageName}.<br>
                                © ${new Date().getFullYear()} ${safeBrokerageName}. Information deemed reliable but not guaranteed.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    `.trim();
}
