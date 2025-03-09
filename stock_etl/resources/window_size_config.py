from sqlalchemy import create_engine, text
import pandas as pd
import os
import json


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
    
    def get_window_size(self, ticker, default=8):
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