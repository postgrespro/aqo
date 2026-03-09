"""Diagnostic script to find source of bad vocab tokens."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))

from utils import load_data
from sensate.pipeline.preprocessing.preprocessing_pipeline import PreprocessingPipeline
from sensate.pipeline.preprocessing.sql_decomposer import decompose_operators_fast

data = load_data('viethq1906', 'generated_sql_samples_from_benchmarks')
p = PreprocessingPipeline()

# --- Stage 1: find bad tokens coming from _tokenize_query on actual fragments ---
trailing_comma_bad = {}
quote_only_bad = {}

for i, sql in enumerate(data[:500]):
    frags = decompose_operators_fast(sql)
    for frag in frags:
        raw_toks = p._tokenize_query(frag)
        for t in raw_toks:
            if t.startswith("'") and t.endswith(','):
                if t not in trailing_comma_bad:
                    trailing_comma_bad[t] = (i, sql, frag, raw_toks)
            elif t.endswith("'") and not t.startswith("'"):
                if t not in quote_only_bad:
                    quote_only_bad[t] = (i, sql, frag, raw_toks)

print(f"=== Trailing-comma bad tokens: {len(trailing_comma_bad)} ===")
for tok, (idx, sql, frag, toks) in list(trailing_comma_bad.items())[:3]:
    print(f"  Token   : {repr(tok)}")
    print(f"  SQL idx : {idx}")
    print(f"  Fragment: {repr(frag[:120])}")
    print(f"  All toks: {[repr(t) for t in toks]}")
    print()

print(f"=== End-quote-only bad tokens: {len(quote_only_bad)} ===")
for tok, (idx, sql, frag, toks) in list(quote_only_bad.items())[:3]:
    print(f"  Token   : {repr(tok)}")
    print(f"  Fragment: {repr(frag[:120])}")
    print(f"  All toks: {[repr(t) for t in toks]}")
    print()

# --- Stage 2: check full pipeline output for surviving bad tokens ---
corpus = p(data[:500])
bad_in_output = set()
for sent in corpus:
    for tok in sent:
        if "'" in tok:
            bad_in_output.add(tok)

print(f"=== Bad tokens surviving full pipeline (500 samples): {len(bad_in_output)} ===")
for t in sorted(bad_in_output)[:10]:
    print(f"  {repr(t)}")
