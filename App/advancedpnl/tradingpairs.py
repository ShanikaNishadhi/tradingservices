trading_pairs = {
    "symbols": [
        {
            "symbol": "AIAUSDT",
            "enabled": True,
            "price_precision": 7,
            "quantity_precision": 0,

            # PNLGap configuration (parent strategy)
            "pnlgap": {
                "long_position_size": 5,
                "short_position_size": 5,
                "long_order_threshold_percent": 3.0,   # Create LONG when price > max + 2%
                "short_order_threshold_percent": 3.0,  # Create SHORT when price < min - 2%
                "profit_threshold_percent": 3.0,       # Close period when net PNL >= 2% of position value
                "leverage": 5
            },

            # SimpleTrends configuration (child strategy - range trading)
            "simpletrends": {
                "long_position_size": 2,
                "short_position_size": 2,
                "long_order_threshold_percent": 3.0,   # ST LONG trigger (price rises from st_min by 1%)
                "short_order_threshold_percent": 3.0,  # ST SHORT trigger (price drops from st_max by 1%)
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
            "symbol": "ASTERUSDT",
            "enabled": True,
            "price_precision": 7,
            "quantity_precision": 0,

            # PNLGap configuration (parent strategy)
            "pnlgap": {
                "long_position_size": 40,
                "short_position_size": 40,
                "long_order_threshold_percent": 2.0,   # Create LONG when price > max + 3%
                "short_order_threshold_percent": 2.0,  # Create SHORT when price < min - 3%
                "profit_threshold_percent": 3.0,       # Close period when net PNL >= 3% of position value
                "leverage": 5
            },

            # SimpleTrends configuration (child strategy - range trading)
            "simpletrends": {
                "long_position_size": 20,
                "short_position_size": 20,
                "long_order_threshold_percent": 2.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 2.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 2.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 2.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 0.5,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 2.0,    # Prevent orders too close forward
                "backward_order_block_percent": 2.0,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20                # Max ST SHORT orders
            }
        },
        {
            "symbol": "FILUSDT",
            "enabled": True,
            "price_precision": 3,
            "quantity_precision": 1,

            # PNLGap configuration (parent strategy)
            "pnlgap": {
                "long_position_size": 17,
                "short_position_size": 17,
                "long_order_threshold_percent": 3.0,   # Create LONG when price > max + 3%
                "short_order_threshold_percent": 3.0,  # Create SHORT when price < min - 3%
                "profit_threshold_percent": 3.0,       # Close period when net PNL >= 3% of position value
                "leverage": 5
            },

            # SimpleTrends configuration (child strategy - range trading)
            "simpletrends": {
                "long_position_size": 7,
                "short_position_size": 7,
                "long_order_threshold_percent": 3.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 3.0,  # ST SHORT trigger (price drops from st_max by 3%)
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
