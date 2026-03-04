trading_pairs = {
    "symbols": [
        {
            "symbol": "COTIUSDT",
            "enabled": False,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 5,
            "quantity_precision": 0,

            # PNLGap configuration (parent strategy)
            "pnlgap": {
                "long_position_size": 1000,
                "short_position_size": 1000,
                "long_first_order_threshold_percent": 2.0,   # First LONG entry from reference price
                "short_first_order_threshold_percent": 2.0,  # First SHORT entry from reference price
                "long_order_threshold_percent": 3.0,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 2.0,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 2.0,        # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 3.0,       # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                # Stop-loss for PNL orders: None=DISABLED, or percentage (e.g., 25.0). First LONG/SHORT never have stop-loss.
                "avoid_onesided_orders": True,               # Prevent creating more orders on one side when other side doesn't exist
                "leverage": 5
            },

            # SimpleTrends configuration (child strategy - range trading)
            "simpletrends": {
                "long_position_size": 500,
                "short_position_size": 500,
                "long_order_threshold_percent": 2.0,   # ST LONG trigger (price rises from st_min by 2%)
                "short_order_threshold_percent": 3.0,  # ST SHORT trigger (price drops from st_max by 2%)
                "long_profit_threshold_percent": 3.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 3.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 1.0,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 3.0,    # Prevent orders too close forward
                "backward_order_block_percent": 3.0,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20                # Max ST SHORT orders
            }
        },
        {
            "symbol": "JCTUSDT",
            "enabled": False,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 7,
            "quantity_precision": 0,

            # PNLGap configuration (parent strategy)
            "pnlgap": {
                "long_position_size": 5000,
                "short_position_size": 5000,
                "long_first_order_threshold_percent": 2.0,   # First LONG entry from reference price
                "short_first_order_threshold_percent": 2.0,  # First SHORT entry from reference price
                "long_order_threshold_percent": 3.0,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 3.0,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 4.0,        # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 4.0,       # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                # Stop-loss for PNL orders: None=DISABLED, or percentage (e.g., 25.0). First LONG/SHORT never have stop-loss.
                "avoid_onesided_orders": True,               # Prevent creating more orders on one side when other side doesn't exist
                "leverage": 5
            },

            # SimpleTrends configuration (child strategy - range trading)
            "simpletrends": {
                "long_position_size": 2500,
                "short_position_size": 2500,
                "long_order_threshold_percent": 3.0,   # ST LONG trigger (price rises from st_min by 2%)
                "short_order_threshold_percent": 3.0,  # ST SHORT trigger (price drops from st_max by 2%)
                "long_profit_threshold_percent": 3.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 3.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 1.0,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 3.0,    # Prevent orders too close forward
                "backward_order_block_percent": 3.0,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20                # Max ST SHORT orders
            }
        }
    ],
}
