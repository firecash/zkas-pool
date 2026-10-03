/* Connection generator only: never changes the pool or submits a transaction. */
(() => {
  'use strict';
  const labels = {native: 'ZKas — native', kaspa: 'Kaspa + ZKas'};
  const alphabet = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';
  function address(raw, prefixes, shielded = false) {
    const value = raw.trim();
    const [prefix, payload, extra] = value.split(':');
    if (extra !== undefined || !prefixes.includes(prefix) || !payload || payload.length > 128 || payload.length < 10) {
      throw new Error(`Expected an address beginning with ${prefixes.join(': or ')}:`);
    }
    const digits = [...payload].map(c => alphabet.indexOf(c));
    if (digits.some(v => v < 0)) throw new Error('Invalid address characters.');
    let checksum = 1n;
    const generators = [0x98f2bc8e61n, 0x79b76d99e2n, 0xf33e5fb3c4n, 0xae2eabe2a8n, 0x1e4f43e470n];
    for (const digit of [...prefix].map(c => c.charCodeAt(0) & 31).concat(0, digits)) {
      const high = checksum >> 35n;
      checksum = ((checksum & 0x07ffffffffn) << 5n) ^ BigInt(digit);
      generators.forEach((g, i) => { if ((high & (1n << BigInt(i))) !== 0n) checksum ^= g; });
    }
    if (checksum !== 1n) throw new Error('Address checksum is invalid. Check the pasted address.');
    let accumulator = 0, bits = 0;
    const bytes = [];
    for (const digit of digits.slice(0, -8)) {
      accumulator = ((accumulator << 5) | digit) & 65535;
      bits += 5;
      if (bits >= 8) { bits -= 8; bytes.push((accumulator >> bits) & 255); }
    }
    const sizes = shielded ? {9: 44} : {0: 33, 1: 34, 8: 33};
    if (bits >= 5 || (accumulator & ((1 << bits) - 1)) !== 0 || sizes[bytes[0]] !== bytes.length) {
      throw new Error(shielded ? 'Use a shielded Orchard ZKas receiving address.' : 'Unsupported address version or length.');
    }
    return value;
  }
  function credentials(option, values) {
    if (!option || !Object.hasOwn(labels, option.mode) || !Number.isInteger(option.port) || option.port < 1 || option.port > 65535) {
      throw new Error('Select an available mining port.');
    }
    const host = values.host.trim();
    if (!/^(?:[a-zA-Z0-9.-]+|\[[0-9a-fA-F:]+\])$/.test(host)) throw new Error('Enter a hostname or IP, without a protocol or port.');
    const parsed = new URL(`http://${host}:${option.port}`);
    if (!parsed.hostname) throw new Error('Invalid server hostname.');
    const wallet = address(values.zkas, ['zkas', 'firecash', 'zkasdev', 'firecashdev', 'zkastest', 'firecashtest', 'zkassim', 'firecashsim'], true);
    const worker = values.worker.trim();
    if (worker && !/^[A-Za-z0-9_-]{1,64}$/.test(worker)) throw new Error('Worker name: up to 64 letters, digits, underscores or hyphens.');
    let password = 'x';
    if (option.mode !== 'native') {
      const prefixes = ['kaspa', 'kaspatest', 'kaspadev', 'kaspasim'];
      if (!prefixes.includes(option.parentPrefix)) throw new Error('Parent payout network is unavailable.');
      password = address(values[option.mode], [option.parentPrefix]);
    }
    return {url: `stratum+tcp://${host}:${option.port}`, username: wallet + (worker ? `.${worker}` : ''), password};
  }
  const api = {address, credentials};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof document === 'undefined') return;
  const form = document.getElementById('miningSetup');
  if (!form) return;
  const select = document.getElementById('miningMode');
  const error = document.getElementById('miningSetupError');
  const output = document.getElementById('miningCredentials');
  const copy = document.getElementById('miningCopy');
  let options = [], result = null;
  function render() {
    const option = options[Number(select.value)];
    document.getElementById('kaspaPayoutField').hidden = option?.mode !== 'kaspa';
    document.getElementById('miningMergeNote').hidden = !option || option.mode === 'native';
    document.getElementById('parentNetwork').textContent = option?.parentPrefix ? `Payout network: ${option.parentPrefix}:` : '';
    result = null; output.hidden = true; copy.disabled = true;
    if (!option) return;
    try {
      result = credentials(option, Object.fromEntries(new FormData(form)));
      document.getElementById('miningUrl').textContent = result.url;
      document.getElementById('miningUsername').textContent = result.username;
      document.getElementById('miningPassword').textContent = result.password;
      error.textContent = ''; output.hidden = false; copy.disabled = false;
    } catch (e) { error.textContent = e.message; }
  }
  form.addEventListener('input', render);
  form.addEventListener('submit', e => { e.preventDefault(); render(); });
  copy.addEventListener('click', async () => {
    if (!result) return;
    try {
      await navigator.clipboard.writeText(`URL: ${result.url}\nWorker: ${result.username}\nPassword: ${result.password}`);
      error.textContent = 'Connection settings copied.';
    } catch (_) { error.textContent = 'Clipboard unavailable. Select and copy the values below.'; }
  });
  document.getElementById('miningHost').value = location.hostname;
  fetch('api/mining', {cache: 'no-store'}).then(r => {
    if (!r.ok) throw new Error('Connection metadata unavailable.');
    return r.json();
  }).then(data => {
    if (!Array.isArray(data)) throw new Error('Invalid connection metadata.');
    options = data.filter(o => Object.hasOwn(labels, o.mode) && Number.isInteger(o.port));
    select.replaceChildren(...options.map((o, i) => new Option(`${labels[o.mode]} · port ${o.port}`, String(i))));
    select.disabled = options.length === 0;
    if (!options.length) throw new Error('No explicitly configured mining modes are available.');
    render();
  }).catch(e => { error.textContent = e.message; select.disabled = true; });
})();
