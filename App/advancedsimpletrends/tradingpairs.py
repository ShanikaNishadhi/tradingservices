trading_pairs = {
    "symbols": [
        {
            "symbol": "UNIUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 4,
            "quantity_precision": 0,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 7,
                "short_position_size": 7,
                "long_first_order_threshold_percent": 20.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 20.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 4,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 4,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 3,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 3,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 3,
                "short_position_size": 3,
                "long_order_threshold_percent": 3.0,   # ST LONG trigger (price rises from st_min by 2%)
                "short_order_threshold_percent": 3.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 3.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 3.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 1.0,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 3.0,    # Prevent orders too close forward
                "backward_order_block_percent": 2.0,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "GIGGLEUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 5,
            "quantity_precision": 2,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 0.4,
                "short_position_size": 0.4,
                "long_first_order_threshold_percent": 20.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 20.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 4,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 4,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 3,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 3,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 0.15,
                "short_position_size": 0.15,
                "long_order_threshold_percent": 2.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 2.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 2.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 2.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 0.5,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 2.0,    # Prevent orders too close forward
                "backward_order_block_percent": 2.0,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "UAIUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 7,
            "quantity_precision": 0,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 250,
                "short_position_size": 250,
                "long_first_order_threshold_percent": 20.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 20.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 4,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 4,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 3,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 3,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 100,
                "short_position_size": 100,
                "long_order_threshold_percent": 3.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 3.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 3.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 3.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 1.0,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 3.0,    # Prevent orders too close forward
                "backward_order_block_percent": 3.0,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "XRPUSDT",
            "enabled": False,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 4,
            "quantity_precision": 1,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 25,
                "short_position_size": 25,
                "long_first_order_threshold_percent": 25.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 25.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 3,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 3,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 1,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 1,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 10,
                "short_position_size": 10,
                "long_order_threshold_percent": 1,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 1,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 1,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 1,  # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 0.3,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 1.0,    # Prevent orders too close forward
                "backward_order_block_percent": 1.0,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "AVAXUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 4,
            "quantity_precision": 0,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 3,
                "short_position_size": 3,
                "long_first_order_threshold_percent": 25.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 20.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 3,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 3,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 2,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 2,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 1,
                "short_position_size": 1,
                "long_order_threshold_percent": 1.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 1.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 1.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 1.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 0.3,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 1.0,    # Prevent orders too close forward
                "backward_order_block_percent": 1.0,   # Prevent orders too close backward
                "long_order_limit": 0,                 # Max ST LONG orders - DISABLED
                "short_order_limit": 20,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "SUIUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 6,
            "quantity_precision": 1,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 30,
                "short_position_size": 30,
                "long_first_order_threshold_percent": 25.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 25.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 3,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 3,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 2,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 2,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 10,
                "short_position_size": 10,
                "long_order_threshold_percent": 1.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 1.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 1.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 1.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 0.3,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 1.0,    # Prevent orders too close forward
                "backward_order_block_percent": 1.0,   # Prevent orders too close backward
                "long_order_limit": 0,                 # Max ST LONG orders - DISABLED
                "short_order_limit": 20,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "ASTERUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 7,
            "quantity_precision": 0,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 50,
                "short_position_size": 50,
                "long_first_order_threshold_percent": 30.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 30.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 3,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 3,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 3,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 3,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 20,
                "short_position_size": 20,
                "long_order_threshold_percent": 1.2,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 1.2,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 1.2,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 1.2, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 0.3,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 1.2,    # Prevent orders too close forward
                "backward_order_block_percent": 1.2,   # Prevent orders too close backward
                "long_order_limit": 20,                # Max ST LONG orders
                "short_order_limit": 20,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "CROSSUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 7,
            "quantity_precision": 0,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 400,
                "short_position_size": 400,
                "long_first_order_threshold_percent": 30.0,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 30.0,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 3.0,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 3,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 3,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 3,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 3,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 150,
                "short_position_size": 150,
                "long_order_threshold_percent": 2.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 2.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 3.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 3.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 1.0,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 2.0,    # Prevent orders too close forward
                "backward_order_block_percent": 2.0,   # Prevent orders too close backward
                "long_order_limit": 0,                # Max ST LONG orders
                "short_order_limit": 30,               # Max ST SHORT orders
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        },
        {
            "symbol": "STRKUSDT",
            "enabled": True,
            "continue_periods": False,  # Set to False to stop creating new periods after current one closes
            "price_precision": 7,
            "quantity_precision": 1,
            "leverage": 5,  # Leverage at symbol level

            # PNLGap configuration (parent strategy - RARE, large thresholds)
            "pnlgap": {
                "long_position_size": 200,
                "short_position_size": 200,
                "long_first_order_threshold_percent": 2,   # First LONG entry from reference price (LARGE!)
                "short_first_order_threshold_percent": 2,  # First SHORT entry from reference price (LARGE!)
                "pnl_gap_percent": 2,                        # Gap between opposing PNL zones once first side created
                "long_order_threshold_percent": 4,         # Create subsequent LONGs when price > max + threshold
                "short_order_threshold_percent": 4,        # Create subsequent SHORTs when price < min - threshold
                "long_profit_threshold_percent": 50,         # Close period when LONG is winning with this profit %
                "short_profit_threshold_percent": 50,        # Close period when SHORT is winning with this profit %
                "pnl_stoploss_percent": None,                 # Stop-loss for PNL orders: None=DISABLED
            },

            # SimpleTrends configuration (child strategy - PRIMARY TRADING)
            "simpletrends": {
                "long_position_size": 100,
                "short_position_size": 100,
                "long_order_threshold_percent": 2.0,   # ST LONG trigger (price rises from st_min by 3%)
                "short_order_threshold_percent": 2.0,  # ST SHORT trigger (price drops from st_max by 3%)
                "long_profit_threshold_percent": 2.0,  # ST LONG trailing stop activation
                "short_profit_threshold_percent": 2.0, # ST SHORT trailing stop activation
                "trailing_stop_callback_rate": 0.5,    # Trailing stop callback %
                "stop_loss_percent": None,             # Optional stop loss
                "forward_order_block_percent": 2.0,    # Prevent orders too close forward
                "backward_order_block_percent": 2.0,   # Prevent orders too close backward
                "long_order_limit": 0,                 # Max ST LONG orders - DISABLED
                "short_order_limit": 0,                # Max ST SHORT orders - DISABLED
                "enable_pnl_barrier": False            # If True, ST LONGs only below PNL LONG entry, ST SHORTs only above PNL SHORT entry
            }
        }
    ],
}
