from sklearn.feature_selection import VarianceThreshold
import pandas
from adplogger import logger
import time
from datetime import datetime
import sys


class AdpFeatureSelector:

    def __init__(self):
        self.author = 'Constructor for dmon-adp  feature selection methods'

    def varianceSelection(self, df, threashold=.8):
        if not isinstance(df, pandas.core.frame.DataFrame):
            logger.error('[%s] : [ERROR] Variance selection only possible on Dataframe not %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(df))
            sys.exit(1)
        sel = VarianceThreshold(threshold=(threashold * (1 - threashold)))
        sel.fit_transform(df)
        return df[[c for (s, c) in zip(sel.get_support(), df.columns.values) if s]]

