import pandas as pd
import logging

logger = logging.getLogger(__name__)

class TechnicalIndicatorEngine:
    """Computes technical indicators like 50 DMA and 200 DMA on historical prices."""

    @staticmethod
    def compute_dmas(df: pd.DataFrame) -> pd.DataFrame:
        """
        Expects a DataFrame with 'close' column.
        Appends 'dma_50' and 'dma_200' to the DataFrame.
        """
        if df.empty or 'close' not in df.columns:
            logger.warning("Indicator engine: DataFrame empty or missing 'close' column.")
            return df

        # Ensure numeric close column
        df['close'] = pd.to_numeric(df['close'], errors='coerce')

        # Calculate 50 DMA
        df['dma_50'] = df['close'].rolling(window=50, min_periods=1).mean().round(2)

        # Calculate 200 DMA
        df['dma_200'] = df['close'].rolling(window=200, min_periods=1).mean().round(2)

        return df
