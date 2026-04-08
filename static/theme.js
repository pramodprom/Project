// Applies the saved theme ASAP (before paint) and wires up the theme toggle button.
// Theme is stored in localStorage as: theme = 'dark' | 'light'.
(function(){
	const STORAGE_KEY = 'theme';

	function getStored(){
		try { return localStorage.getItem(STORAGE_KEY); } catch { return null; }
	}

	function apply(mode){
		const m = (mode === 'light') ? 'light' : 'dark';
		try{
			document.documentElement.setAttribute('data-theme', m);
		}catch{}
		try{
			const btn = document.getElementById('themeToggle');
			if (btn){
				btn.setAttribute('aria-pressed', String(m === 'light'));
				const lbl = btn.querySelector('[data-theme-label]');
				if (lbl) lbl.textContent = (m === 'light') ? 'Light' : 'Dark';
			}
		}catch{}
	}

	// Initial apply (ASAP)
	apply(getStored() || 'dark');

	// After DOM is ready, wire toggle if present
	document.addEventListener('DOMContentLoaded', function(){
		const btn = document.getElementById('themeToggle');
		if (!btn) return;
		btn.addEventListener('click', function(){
			const curr = (document.documentElement.getAttribute('data-theme') || 'dark');
			const next = (curr === 'light') ? 'dark' : 'light';
			try { localStorage.setItem(STORAGE_KEY, next); } catch {}
			apply(next);
		});
	});
})();
