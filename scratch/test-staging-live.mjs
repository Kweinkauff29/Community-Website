async function test() {
    const base = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
    
    // 1. Bootstrap
    const bootRes = await fetch(base + '/idx/v1/bootstrap?site=ursulaweinkauff-com', {
        headers: { 
            'Accept': 'application/json',
            'Origin': 'http://ursulaweinkauff.com'
        }
    });
    console.log('Bootstrap status:', bootRes.status);
    const boot = await bootRes.json();
    console.log('Bootstrap response:', JSON.stringify(boot));

    if (!boot.session) {
        console.error('No session returned');
        return;
    }

    // 2. Search for Ursula's listings (agent=633942)
    const agentRes = await fetch(base + '/idx/v1/search?site=ursulaweinkauff-com&agent=633942&session=' + encodeURIComponent(boot.session), {
        headers: { 'Origin': 'http://ursulaweinkauff.com' }
    });
    console.log('Agent search status:', agentRes.status);
    const agentText = await agentRes.text();
    console.log('Agent search body:', agentText);
    let agentData = {};
    try { agentData = JSON.parse(agentText); } catch {}
    console.log('Agent 633942 results count:', agentData.data?.length);
    if (agentData.data?.length > 0) {
        const item = agentData.data[0];
        console.log('Sample listing:', {
            ListingKey: item.ListingKey,
            Address: item.UnparsedAddress,
            Price: item.ListPrice,
            AgentName: item.ListAgentFullName,
            AgentPhotoUrl: item.AgentPhotoUrl,
            OpenHouse: item.OpenHouse
        });
    }

    // 3. Search with city and price range
    const cityRes = await fetch(base + '/idx/v1/search?site=ursulaweinkauff-com&city=Fort%20Myers%20Beach&minPrice=350000&maxPrice=500000&session=' + encodeURIComponent(boot.session));
    const cityData = await cityRes.json();
    console.log('Fort Myers Beach count:', cityData.data?.length, 'Total:', cityData.pagination?.total);
    if (cityData.data?.length > 0) {
        console.log('First 2 listings:', cityData.data.slice(0, 2).map(l => ({
            Price: l.ListPrice,
            Address: l.UnparsedAddress,
            City: l.City,
            AgentPhotoUrl: l.AgentPhotoUrl
        })));
    }

    // 4. Search open houses
    const ohRes = await fetch(base + '/idx/v1/search?site=ursulaweinkauff-com&openHouses=true&session=' + encodeURIComponent(boot.session));
    const ohData = await ohRes.json();
    console.log('Open houses count:', ohData.data?.length);
    if (ohData.data?.length > 0) {
        console.log('Open house sample:', {
            Address: ohData.data[0].UnparsedAddress,
            OpenHouse: ohData.data[0].OpenHouse
        });
    }
}
test().catch(console.error);
