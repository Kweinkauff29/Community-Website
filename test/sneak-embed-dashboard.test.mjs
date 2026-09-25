import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { generateEmbedSnippets } from '../sneak-admin/embed-generator.js';
import { renderMemberUI } from '../sneak-member/ui.js';
import { renderAdminHtml } from '../sneak-admin/ui.js';

test('copied quick-search URLs stay on the embedding website, never guessed domains', () => {
  const embed = generateEmbedSnippets('unrelated-tenant-key', ['*.example.com', 'localhost', 'homes.example.com'], {}, {SNEAK_ENV: 'production'});
  assert.equal(embed.searchRedirectUrl, '/quick-search');
  assert.match(embed.snippets.search_bar.htmlSnippet, /data-redirect-url="\/quick-search"/);
  assert.doesNotMatch(embed.snippets.search_bar.htmlSnippet, /http:|unrelated-tenant-key.com|staging/);
  assert.equal(generateEmbedSnippets('tenant', []).searchRedirectUrl, '/quick-search');
});

test('dashboard inline scripts parse and member builder creates usable grid and quick-search snippets', () => {
  const values = {
    builderWidgetType: {value:'quick-search'}, builderRedirect: {value:'https://example.com/find?source=home'},
    builderHeading: {value:'Find your home'}, builderRedirectGroup:{style:{}}, customGeneratedSnippet:{}
  };
  const context = vm.createContext({
    document: {getElementById: id => values[id] || {addEventListener(){},value:'',checked:false}},
    window: {addEventListener(){}}, console,
  });
  for (const html of [renderMemberUI(), renderAdminHtml({SNEAK_ENV:'production'})]) {
    for (const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
  }
  const html=renderMemberUI();
  const script=[...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n');
  // Execute only the builder and its escaping helper; no authentication or API side effects.
  const start=script.indexOf('function updateCustomEmbedCode()');
  const end=script.indexOf('function switchTab(',start);
  vm.runInContext("let memberServingHost='https://idx.example.com'; let memberSiteKey='tenant'; function escapeHtml(s){return String(s).replaceAll('&','&amp;').replaceAll('\"','&quot;');}"+script.slice(start,end),context);
  vm.runInContext('updateCustomEmbedCode()',context);
  assert.match(values.customGeneratedSnippet.innerText,/data-widget="quick-search"/);
  assert.match(values.customGeneratedSnippet.innerText,/data-redirect-url="https:\/\/example.com\/find\?source=home"/);
  assert.ok(values.customGeneratedSnippet.innerText.includes('\n<div '));
  values.builderWidgetType.value='grid';
  vm.runInContext('updateCustomEmbedCode()',context);
  assert.match(values.customGeneratedSnippet.innerText,/data-layout="grid"/);
  assert.match(values.customGeneratedSnippet.innerText,/data-target="#sneak-idx-grid"/);
});
