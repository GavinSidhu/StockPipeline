from sqlalchemy import create_engine, text
import pandas as pd
import datetime

class ThresholdConfig:
    """Manages the storage and retrieval of buy/sell threshold configurations."""
    
    def __init__(self, db_config):
        """Initialize with database configuration."""
        self.db_config = db_config
        self.engine = create_engine(
            f"postgresql://{self.db_config.username}:{self.db_config.password}@"
            f"{self.db_config.host}:{self.db_config.port}/{self.db_config.database}"
        )
        self._ensure_config_table()
    
    def _ensure_config_table(self):
        """Ensure the threshold configuration table exists."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS stock
            """))
            conn.commit()
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stock.threshold_config (
                    ticker VARCHAR(10) PRIMARY KEY,
                    buy_threshold FLOAT NOT NULL,
                    sell_threshold FLOAT NOT NULL,
                    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
                    change_reason TEXT,
                    performance_score FLOAT
                )
            """))
            conn.commit()
    
    def get_thresholds(self, ticker, defaults=None):
        """
        Get the buy/sell thresholds for a specific ticker.
        
        Args:
            ticker: Stock ticker symbol
            defaults: Optional dictionary with default 'buy_threshold' and 'sell_threshold'
                    If not provided, defaults to 5% for both
        
        Returns:
            Dictionary with buy_threshold and sell_threshold values
        """
        if defaults is None:
            defaults = {
                'buy_threshold': 5.0,  # Default 5% above min for buy signals
                'sell_threshold': 5.0   # Default 5% below max for sell signals
            }
            
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT buy_threshold, sell_threshold FROM stock.threshold_config
                WHERE ticker = :ticker
            """), {"ticker": ticker})
            row = result.fetchone()
            
            if row:
                return {
                    'buy_threshold': row[0],
                    'sell_threshold': row[1]
                }
            else:
                # Initialize with defaults if not found
                self.set_thresholds(
                    ticker, 
                    defaults['buy_threshold'], 
                    defaults['sell_threshold'], 
                    "Initial threshold setting"
                )
                return defaults
    
    def set_thresholds(self, ticker, buy_threshold, sell_threshold, reason=None, performance_score=None):
        """
        Set the buy/sell thresholds for a specific ticker.
        
        Args:
            ticker: Stock ticker symbol
            buy_threshold: Percentage above minimum price for buy signals
            sell_threshold: Percentage below maximum price for sell signals
            reason: Optional reason for the change
            performance_score: Optional performance score associated with these thresholds
        """
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO stock.threshold_config 
                (ticker, buy_threshold, sell_threshold, last_updated, change_reason, performance_score)
                VALUES (:ticker, :buy_threshold, :sell_threshold, NOW(), :reason, :score)
                ON CONFLICT (ticker) 
                DO UPDATE SET 
                    buy_threshold = :buy_threshold,
                    sell_threshold = :sell_threshold,
                    last_updated = NOW(),
                    change_reason = :reason,
                    performance_score = :score
            """), {
                "ticker": ticker,
                "buy_threshold": buy_threshold,
                "sell_threshold": sell_threshold,
                "reason": reason,
                "score": performance_score
            })
            conn.commit()
    
    def get_all_configs(self):
        """Get all ticker threshold configurations."""
        return pd.read_sql("SELECT * FROM stock.threshold_config ORDER BY ticker", self.engine)
    
    def update_from_backtest(self, backtest_results, min_improvement_pct=15):
        """
        Update thresholds based on backtest results if improvement is significant.
        
        Args:
            backtest_results: DataFrame with backtest results
            min_improvement_pct: Minimum percentage improvement required to change thresholds
        
        Returns:
            List of changes made
        """
        changes = []
        
        # Process each ticker's backtest results
        for ticker in backtest_results['ticker'].unique():
            ticker_data = backtest_results[backtest_results['ticker'] == ticker]
            
            # Skip if no performance data
            if ticker_data.empty or ticker_data['overall_performance'].max() <= 0:
                continue
            
            # Get current thresholds
            current_thresholds = self.get_thresholds(ticker)
            
            # Find row with current thresholds or closest match
            current_row = ticker_data[
                (ticker_data['buy_threshold'] == current_thresholds['buy_threshold']) & 
                (ticker_data['sell_threshold'] == current_thresholds['sell_threshold'])
            ]
            
            if current_row.empty:
                # If exact match not found, find closest match based on thresholds
                ticker_data['threshold_diff'] = abs(
                    ticker_data['buy_threshold'] - current_thresholds['buy_threshold']
                ) + abs(
                    ticker_data['sell_threshold'] - current_thresholds['sell_threshold']
                )
                current_row = ticker_data.loc[ticker_data['threshold_diff'].idxmin()]
            else:
                current_row = current_row.iloc[0]
            
            current_performance = current_row['overall_performance']
            
            # Find best performing thresholds
            best_row = ticker_data.loc[ticker_data['overall_performance'].idxmax()]
            best_performance = best_row['overall_performance']
            
            # Calculate improvement percentage
            if current_performance > 0:
                improvement_pct = (best_performance - current_performance) / current_performance * 100
            else:
                improvement_pct = float('inf')  # Infinite improvement from zero
            
            # Only change if significant improvement and different thresholds
            if (improvement_pct >= min_improvement_pct and 
                (best_row['buy_threshold'] != current_thresholds['buy_threshold'] or 
                 best_row['sell_threshold'] != current_thresholds['sell_threshold'])):
                
                reason = (f"Automated change: {best_row['buy_threshold']}%/{best_row['sell_threshold']}% "
                         f"thresholds outperformed {current_thresholds['buy_threshold']}%/"
                         f"{current_thresholds['sell_threshold']}% by {improvement_pct:.1f}%")
                
                self.set_thresholds(
                    ticker,
                    float(best_row['buy_threshold']),
                    float(best_row['sell_threshold']),
                    reason=reason,
                    performance_score=float(best_performance)
                )
                
                changes.append({
                    'ticker': ticker,
                    'old_buy_threshold': current_thresholds['buy_threshold'],
                    'old_sell_threshold': current_thresholds['sell_threshold'],
                    'new_buy_threshold': best_row['buy_threshold'],
                    'new_sell_threshold': best_row['sell_threshold'],
                    'improvement_pct': improvement_pct,
                    'reason': reason
                })
        
        return changes
    
    def update_monthly_if_needed(self):
        """
        Check if thresholds need to be updated based on monthly schedule.
        Updates thresholds if they haven't been updated in the current month.
        
        Returns:
            List of tickers that were updated
        """
        # Get current month and year
        now = datetime.datetime.now()
        current_month = now.month
        current_year = now.year
        
        # Get all configs
        configs = self.get_all_configs()
        updated_tickers = []
        
        for _, row in configs.iterrows():
            # Check if this config has been updated this month
            last_updated = pd.to_datetime(row['last_updated'])
            
            if last_updated.month != current_month or last_updated.year != current_year:
                # Needs a monthly update - trigger a backtest or use predefined values
                # For now, we'll just slightly randomize the thresholds to simulate optimization
                import random
                
                # Get a slight variation (+/- 0.5%) of the current thresholds
                new_buy = max(0.5, min(20, row['buy_threshold'] + random.uniform(-0.5, 0.5)))
                new_sell = max(0.5, min(20, row['sell_threshold'] + random.uniform(-0.5, 0.5)))
                
                self.set_thresholds(
                    row['ticker'],
                    new_buy,
                    new_sell,
                    reason=f"Monthly update for {now.strftime('%B %Y')}"
                )
                
                updated_tickers.append(row['ticker'])
        
        return updated_tickers