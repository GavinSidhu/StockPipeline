from sqlalchemy import create_engine, text
import pandas as pd
import datetime


class WindowSizeConfig:
    """Manages the storage and retrieval of window size configurations."""
    
    def __init__(self, db_config):
        """Initialize with database configuration."""
        self.db_config = db_config
        self.engine = create_engine(
            f"postgresql://{self.db_config.username}:{self.db_config.password}@"
            f"{self.db_config.host}:{self.db_config.port}/{self.db_config.database}"
        )
        self._ensure_config_table()
    
    def _ensure_config_table(self):
        """Ensure the configuration table exists."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS stock
            """))
            conn.commit()
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stock.window_config (
                    ticker VARCHAR(10) PRIMARY KEY,
                    window_size INTEGER NOT NULL,
                    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
                    change_reason TEXT,
                    performance_score FLOAT
                )
            """))
            conn.commit()
    
    def get_window_size(self, ticker, default=30):
        """Get the exploration window size for a specific ticker."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT window_size FROM stock.window_config
                WHERE ticker = :ticker
            """), {"ticker": ticker})
            row = result.fetchone()
            
            if row:
                return row[0]
            else:
                # Initialize with default if not found
                self.set_window_size(ticker, default, "Initial setting")
                return default
    
    def set_window_size(self, ticker, window_size, reason=None, performance_score=None):
        """Set the exploration window size for a specific ticker."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO stock.window_config (ticker, window_size, last_updated, change_reason, performance_score)
                VALUES (:ticker, :window_size, NOW(), :reason, :score)
                ON CONFLICT (ticker) 
                DO UPDATE SET 
                    window_size = :window_size,
                    last_updated = NOW(),
                    change_reason = :reason,
                    performance_score = :score
            """), {
                "ticker": ticker,
                "window_size": window_size,
                "reason": reason,
                "score": performance_score
            })
            conn.commit()
    
    def get_all_configs(self):
        """Get all ticker window configurations."""
        return pd.read_sql("SELECT * FROM stock.window_config ORDER BY ticker", self.engine)
    
    def update_from_backtest(self, backtest_results, min_improvement_pct=20):
        """Update window sizes based on backtest results if improvement is significant.
        
        Args:
            backtest_results: DataFrame with backtest results
            min_improvement_pct: Minimum percentage improvement required to change window size
        
        Returns:
            List of changes made
        """
        changes = []
        
        # Get best window for each ticker
        for ticker in backtest_results['ticker'].unique():
            ticker_data = backtest_results[backtest_results['ticker'] == ticker]
            
            # Skip if no performance data
            if ticker_data['overall_performance'].max() <= 0:
                continue
            
            # Get current window size
            current_window = self.get_window_size(ticker)
            current_performance = ticker_data[ticker_data['window_size'] == current_window]['overall_performance'].values
            
            if len(current_performance) == 0:
                # Current window size not in test results
                continue
                
            current_performance = current_performance[0]
            
            # Find best window size
            best_window = ticker_data.loc[ticker_data['overall_performance'].idxmax(), 'window_size']
            best_performance = ticker_data['overall_performance'].max()
            
            # Calculate improvement percentage
            if current_performance > 0:
                improvement_pct = (best_performance - current_performance) / current_performance * 100
            else:
                improvement_pct = float('inf')  # Infinite improvement from zero
            
            # Only change if significant improvement and different window
            if best_window != current_window and improvement_pct >= min_improvement_pct:
                reason = (f"Automated change: {best_window}-day window outperformed {current_window}-day "
                         f"window by {float(improvement_pct):.1f}%")
                
                self.set_window_size(
                    ticker, 
                    int(best_window), 
                    reason=reason,
                    performance_score=float(best_performance)
                )
                
                changes.append({
                    'ticker': ticker,
                    'old_window': current_window,
                    'new_window': best_window,
                    'improvement_pct': improvement_pct,
                    'reason': reason
                })
        
        return changes
    
    def update_monthly_if_needed(self):
        """
        Check if window sizes need to be updated based on monthly schedule.
        Updates window sizes if they haven't been updated in the current month.
        
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
                # This is where we would normally trigger a backtest to find the optimal window size
                # For now, we'll just set a random window size within our desired range
                # In practice, you'd want to run a more sophisticated optimization here
                
                # Window sizes range from 14 days to 6 months (about 180 days)
                # Let's pick from common window sizes in this range
                window_options = [14, 30, 60, 90, 120, 180]
                
                # For demonstration, we'll just pick based on the month
                # In a real implementation, this would be based on backtest results
                index = current_month % len(window_options)
                new_window_size = window_options[index]
                
                self.set_window_size(
                    row['ticker'],
                    new_window_size,
                    reason=f"Monthly update for {now.strftime('%B %Y')}"
                )
                
                updated_tickers.append(row['ticker'])
        
        return updated_tickers