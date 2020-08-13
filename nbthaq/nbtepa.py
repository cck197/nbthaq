import pandas as pd
import numpy as np
from collections import defaultdict
from functools import partial

import haq
from mongoengine.db.base.metaclasses import DocumentMetaclass


class NBTEPA(object):
    def __init__(self, fname):
        df = pd.read_csv(fname)
        to_filter = [
            ("I feel isolated...", "A little bit"),
            ("There are people I can talk to...", "A little bit"),
            ("I feel part of a group of friends...", "Quite a bit"),
        ]
        [df.drop(df[df[k] == v].index, inplace=True) for (k, v) in to_filter]
        df = df.astype("category")
        self.df = df
        classes = [
            key
            for key in dir(haq)
            if type(getattr(haq, key)) is DocumentMetaclass
        ]
        d = {}
        dmap = defaultdict(list)
        for clsname in classes:
            cls = getattr(haq, clsname)
            for f in cls._fields.values():
                if f.choices is not None:
                    dmap[clsname].append(f.verbose_name)
                    d[f.verbose_name] = {name: score for (score, name) in f.choices}
        df.dropna(thresh=5, subset=d.keys(), inplace=True)
        for k in d.keys():
            df[f"{k}_"] = df[k].apply(lambda x: d[k][x])
        self.df, self.d, self.dmap = df, d, dmap

    def get_total_score(self, df, row):
        total = 0
        for k in df.columns:
            if not k in self.d:
                continue
            v = row[k]
            if not pd.isna(v):
                total += max(self.d[k].values())
        return total

    def add_scores_(self, df):
        cols = [f"{k}_" for k in df.columns if k in self.d]
        score_total = df[cols].sum(axis=1, skipna=True)
        score_max = df.apply(partial(self.get_total_score, df), axis=1)
        score_pct = (1 - (score_total / score_max)) * 100
        return score_total, score_max, score_pct

    def add_scores(self, df=None):
        if df is None:
            df = self.df
        df["score_total"], df["score_max"], df["score_pct"] = self.add_scores_(df)
        for (cat, cols) in self.dmap.items():
            dcols = [k for k in df.columns if k.replace("_", "") in cols]
            ddf = df[dcols].copy()
            s = cat.lower()
            (
                df[f"{s}_score_total"],
                df[f"{s}_score_max"],
                df[f"{s}_score_pct"],
            ) = self.add_scores_(ddf)
        return df
