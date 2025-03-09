from dagster import asset, Output, MetadataValue
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import io
import base64

@asset(
    deps=["stock_recommendations"],
    required_resource_keys={"database_config"}
)
def position_sizing(context):
    """Calculate optimal position sizes based on signal strength and available funds."""
    # Get database config
    db_config = context.resources.database_config
    
    # Connect to database
    engine = create_engine(
        f"postgresql://{db_config.username}:{db_config.password}@"
        f"{db_config.host}:{db_config.port}/{db_config.database}"
    )
    
    # Get latest recommendations from the database
    recommendations_df = pd.read_sql("""
        SELECT ticker, date, close, recommendation, 
               exploration_min, exploration_max, 
               price_vs_min, price_vs_max,
               buy_strong, buy_medium, buy_weak,
               sell_strong, sell_medium, sell_weak
        FROM stock.stock_metrics
        ORDER BY ticker
    """, engine)
    
    if recommendations_df.empty:
        context.log.warning("No recommendations found in the database")
        return Output(
            pd.DataFrame(),
            metadata={"error": "No recommendations found"}
        )
    
    # Get configuration from environment or use defaults
    total_allocation_pct = 100  # Total percentage of paycheck to allocate
    max_position_pct = 50  # Maximum percentage allocation for a single position
    min_position_pct = 10  # Minimum percentage allocation for a position with signal
    
    # Calculate confidence scores based on recommendation type and price distance
    recommendations_df['confidence_score'] = 0.0
    
    # For buy signals
    buy_mask = recommendations_df['recommendation'].str.contains('BUY', na=False)
    if buy_mask.any():
        # Calculate confidence based on distance from the minimum
        # The further below the minimum, the higher the confidence
        recommendations_df.loc[buy_mask, 'confidence_score'] = np.abs(
            (recommendations_df.loc[buy_mask, 'exploration_min'] - recommendations_df.loc[buy_mask, 'close']) / 
            recommendations_df.loc[buy_mask, 'exploration_min'] * 100
        )
        
        # Boost confidence for STRONG BUY signals
        strong_buy_mask = recommendations_df['recommendation'].str.contains('STRONG BUY', na=False)
        if strong_buy_mask.any():
            recommendations_df.loc[strong_buy_mask, 'confidence_score'] *= 1.5
            
        # Slightly reduce confidence for WEAK BUY signals
        weak_buy_mask = recommendations_df['recommendation'].str.contains('WEAK BUY', na=False)
        if weak_buy_mask.any():
            recommendations_df.loc[weak_buy_mask, 'confidence_score'] *= 0.7
    
    # For sell signals
    sell_mask = recommendations_df['recommendation'].str.contains('SELL', na=False)
    if sell_mask.any():
        # Calculate confidence based on distance from the maximum
        # The further above the maximum, the higher the confidence
        recommendations_df.loc[sell_mask, 'confidence_score'] = np.abs(
            (recommendations_df.loc[sell_mask, 'close'] - recommendations_df.loc[sell_mask, 'exploration_max']) / 
            recommendations_df.loc[sell_mask, 'exploration_max'] * 100
        )
        
        # Boost confidence for STRONG SELL signals
        strong_sell_mask = recommendations_df['recommendation'].str.contains('STRONG SELL', na=False)
        if strong_sell_mask.any():
            recommendations_df.loc[strong_sell_mask, 'confidence_score'] *= 1.5
            
        # Slightly reduce confidence for WEAK SELL signals
        weak_sell_mask = recommendations_df['recommendation'].str.contains('WEAK SELL', na=False)
        if weak_sell_mask.any():
            recommendations_df.loc[weak_sell_mask, 'confidence_score'] *= 0.7
    
    # For WATCH signals, assign a lower confidence
    watch_mask = recommendations_df['recommendation'].str.contains('WATCH', na=False)
    if watch_mask.any():
        recommendations_df.loc[watch_mask, 'confidence_score'] = 0.5
    
    # Calculate allocations based on confidence scores
    if recommendations_df['confidence_score'].max() > 0:
        # Normalize confidence scores
        normalized_confidence = recommendations_df['confidence_score'] / recommendations_df['confidence_score'].max()
        
        # Calculate recommended allocation percentages using confidence scores
        recommendations_df['allocation_pct'] = normalized_confidence * max_position_pct
        
        # Enforce minimum allocation for positions with any confidence
        min_alloc_mask = (recommendations_df['confidence_score'] > 0) & (recommendations_df['allocation_pct'] < min_position_pct)
        recommendations_df.loc[min_alloc_mask, 'allocation_pct'] = min_position_pct
        
        # Ensure total allocation doesn't exceed 100%
        total_allocation_pct = recommendations_df['allocation_pct'].sum()
        if total_allocation_pct > total_allocation_pct:
            # Scale back proportionally
            scale_factor = total_allocation_pct / total_allocation_pct
            recommendations_df['allocation_pct'] = recommendations_df['allocation_pct'] * scale_factor
            
        # Suggest example dollar amounts based on a sample $1000 paycheck (for illustration only)
        sample_amount = 1000
        recommendations_df['example_amount'] = (recommendations_df['allocation_pct'] / 100) * sample_amount
        
        # Calculate example shares at current prices (for illustration only)
        recommendations_df['example_shares'] = (recommendations_df['example_amount'] / recommendations_df['close']).round(0)
    else:
        # No actionable signals
        recommendations_df['allocation_pct'] = 0
        recommendations_df['allocation_amount'] = 0
        recommendations_df['shares_to_trade'] = 0
        recommendations_df['actual_investment'] = 0
    
    # Create visualization of position sizing
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 7))
    
    # Plot 1: Confidence Score by Ticker
    ax1.bar(recommendations_df['ticker'], recommendations_df['confidence_score'])
    ax1.set_title('Signal Confidence Score by Ticker')
    ax1.set_xlabel('Ticker')
    ax1.set_ylabel('Confidence Score')
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Allocation Percentage by Ticker
    ax2.bar(recommendations_df['ticker'], recommendations_df['allocation_pct'])
    ax2.set_title('Recommended Allocation Percentage of Paycheck')
    ax2.set_xlabel('Ticker')
    ax2.set_ylabel('Allocation %')
    for i, ticker in enumerate(recommendations_df['ticker']):
        allocation = recommendations_df.loc[recommendations_df['ticker'] == ticker, 'allocation_pct'].values[0]
        if allocation > 0:
            ax2.annotate(f'{allocation:.1f}%', 
                       (i, allocation),
                       textcoords="offset points",
                       xytext=(0, 10),
                       ha='center')
    ax2.grid(True, alpha=0.3)
    
    # Save the plot
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format='png', dpi=120)
    buf.seek(0)
    plt.close(fig)
    
    # Save position sizing recommendations to database
    recommendations_df.to_sql('position_sizing', engine, schema='stock', if_exists='replace', index=False)
    
    # Create a markdown summary
    markdown_summary = f"## Position Sizing Recommendations\n\n"
    markdown_summary += f"Percentage allocations of your paycheck based on signal strength\n\n"
    markdown_summary += "| Ticker | Signal | Confidence | Allocation % | Example @ $1000 |\n"
    markdown_summary += "|--------|--------|------------|--------------|----------------|\n"
    
    for _, row in recommendations_df.iterrows():
        action_type = "BUY" if "BUY" in row['recommendation'] else "SELL" if "SELL" in row['recommendation'] else "HOLD"
        markdown_summary += f"| {row['ticker']} | {action_type} | {row['confidence_score']:.2f} | {row['allocation_pct']:.1f}% | ${row['example_amount']:.2f} |\n"
    
    markdown_summary += f"\n\nTotal Allocation: {recommendations_df['allocation_pct'].sum():.1f}% of paycheck"
    
    return Output(
        recommendations_df,
        metadata={
            "markdown_summary": MetadataValue.md(markdown_summary),
            "allocation_plot": MetadataValue.md(f"![Allocation Plot](data:image/png;base64,{base64.b64encode(buf.getvalue()).decode()})"),
            "total_allocation_pct": float(recommendations_df['allocation_pct'].sum())
        }
    )