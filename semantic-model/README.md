<div align="center">

# MultiSense Word2Vec (DEV)

Lightweight multi-sense Word2Vec playground. Interface & layout NOT stable yet.

</div>

---

## 1. Python Version

Requires / tested on: **Python 3.11.5**

Check:
```bash
python3 --version
```
Consider an isolated environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
```

## 2. Install & Run (current workflow)

From repo root or any location:
```bash
cd multisense-word2vec
pip install -r requirement.txt
cd src
python3 main.py
```

One‑liner:
```bash
cd multisense-word2vec && pip install -r requirement.txt && cd src && python3 main.py
```

## 3. Minimal Structure

```
multisense-word2vec/
	requirement.txt      # Dependencies (will grow)
	config.yaml          # (Optional) runtime/training config
	output/              # Generated artifacts (may start empty)
	src/
		main.py            # Entry script
		utils.py           # Helpers
		sensate/           # Core modules (WIP)
```

## 4. Development Notes

* APIs & folder layout can change without notice.
* Prefer config-driven parameters (add to `config.yaml`).
* Keep commits small; no large mixed changes.
* Avoid hardcoding paths; use relative or config.

## 5. Troubleshooting

Imports failing when running from repo root:
```bash
cd multisense-word2vec/src
python3 main.py
```
Or set `PYTHONPATH`:
```bash
export PYTHONPATH="$(pwd)"
```

Dependency issues:
```bash
python3 -m pip install --upgrade pip
pip install -r requirement.txt --force-reinstall
```

See what got installed:
```bash
pip list
```
