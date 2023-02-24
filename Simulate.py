
# contains Class simulate PnL in CFD mode ; below : Class OrderManager ;
# and finally some util for computing drawdown and other metrics

class CFDSimulate():
    """ Main backtester for allocation choice for symbols chosen in experiment['symbol_list']
        Margin/wallet in risk free asset (dollar or usdt say); reproduced trading on USDT future perp,
        with CFD (contracts on future difference)
        *** DOES NOT INCLUDE FUNDING ***
        Let n = len(symbol_list), then
        :param experiment : dict, from class experiment, specifying multiple parameters
        :param allocation_long : array, shape (L,P) L is the number of timesteps; P = n+1 ; first column is timestamps
        :param allocation_short : array, shape (L,P) L is the number of timesteps; P = n+1 ; first column is timestamps
        :param input_data, 5 arrays of shape (L,P) for opens, highs, lows, closes, volumes
        opens is eg. (timestamp, open_symbol_1, open_symbole_2, etc)
        :param delta_highs and lows are a list of limit order prices below (delta_lows) and above (delta_highs)
               to be submitted. delta_hihs = [delta_high(symbol1) delta_high(symbol2), ...]
               For each symbol, delta_high(symbol) is an array of shape (L, 1+ N) where N is the number of limit orders
               and first channel is the timestamp.
        """

    def __init__(self, experiment,
                 allocation_long, allocation_short,
                 input_data, delta_highs, delta_lows,
                 position_mode='directional'):

        # exchange usually propose hedge mode where you can simultaneously hold longs or short
        assert position_mode == 'directional', 'hedge mode not included in this repo'
        self.position_mode = position_mode

        assert allocation_long.shape == allocation_short.shape
        assert delta_highs[0].shape[0] == allocation_long.shape[
            0], 'not the same number of timestamps in deltas and allocs'

        opens, highs, lows, closes, volumes = input_data

        assert allocation_long.shape[0] == opens.shape[0], 'critical shape issue axis 0 in Simu'
        assert allocation_long.shape[1] == opens.shape[1] + 1, 'critical shape issue axis 1 in Simu'
        assert allocation_long[-1, 0] == opens[-1, 0], 'non equal ending timestamps in Simu (1)'
        assert allocation_long[0, 0] == opens[0, 0], 'non equal beginning timestamps in Simu (1)'
        assert delta_highs[0][0, 0] == opens[0, 0], 'non equal ending timestamps in Simu (2)'
        assert delta_highs[0][0, 0] == delta_lows[0][0, 0], 'non equal beginning timestamps in Simu (2)'

        self.experiment = experiment
        # timestamps were used only in order to check synch. now get rid of them
        self.allocation_long = allocation_long[:, 1:]
        self.allocation_short = allocation_short[:, 1:]

        self.opens = opens[:, 1:]
        self.highs = highs[:, 1:]
        self.lows = lows[:, 1:]
        self.closes = closes[:, 1:]

        self.delta_highs = delta_highs
        self.delta_lows = delta_lows

        self.L = self.allocation_long.shape[0]  # number of timestamps
        self.np = self.allocation_long.shape[1] -1  # number of symbols

        # on solde au denier timestamp: #todo faudra le market celui la
        self.symbols = self.experiment['symbol_list']

        self.num_orders = []  # how many limit orders do we have per symbol (list)
        for k, symbol in enumerate(self.symbols):
            self.num_orders.append(delta_highs[k].shape[1] - 1)

        self.fee_market = self.experiment['order_parameters']['fee_market']
        self.fee_limit = self.experiment['order_parameters']['fee_limit']
        self.order_type = self.experiment['order_type']

        self.drawdown_window = self.experiment['drawdown_window']  # in number of timestamps
        # unless you dont have enough data points, in that case, default is half:
        if self.drawdown_window >= self.L:
            self.drawdown_window = self.L // 2

        self.ticksizes = [0.0000000001, 0.000000001]

    def init_local_quantities(self, i):
        '''get relevant quantities at time i'''
        self.localmargin = self.margin[-1]
        self.upnl = 0  # unrealized pnl
        self.rpnl = 0  # realized pnl
        self.feepaid = 0
        self.local_delta_high = []
        self.local_delta_low = []
        for k, symbol in enumerate(self.symbols):
            self.local_delta_high.append(self.delta_highs[k][i, 1:])  # cf timestamp at pos 0
            self.local_delta_low.append(self.delta_lows[k][i, 1:])  # same

    def compute_qties(self, i, alloc):
        '''compute qties of each symbol at time i given alloc'''
        target_quantities = []
        for k in range(self.np):
            target_quantities.append(self.localmargin * alloc[k] / self.opens[i, k])
        return target_quantities

    def run_simu(self):
        # init accounting variables:
        self.previous_entry_price = [None] * self.np
        self.current_quantities = [0] * self.np

        # init margin
        self.margin = [100]  # lets have 1000 $ as initial margin (includes upnl, == equity)
        self.wb = [100]  # wallet balance (does not include upnl, only rpnl)
        ordermanager = Order_Manager(self.position_mode, self.order_type, self.np, self.ticksizes)

        # main loop
        for i in range(self.L):
            self.init_local_quantities(i)
            if self.localmargin <= 1:  # you lost 99% : say your broke. This saves some time
                self.margin.append(0)
                continue

            self.new_alloc_long = self.allocation_long[i, :]
            self.new_alloc_short = self.allocation_short[i, :]

            #if i > 3000 and i < 3050:
            #    print('b', self.new_alloc_long - self.new_alloc_short)
            # if both alloc long and short are = 1, say, then we will simultaneously put orders below and above mark price
            self.target_quantities_long = self.compute_qties(i, self.new_alloc_long)
            self.target_quantities_short = self.compute_qties(i, self.new_alloc_short)

            ordermanager.reinit_all_pendings()  # we cancel all pending orders at the beginning of each candle

            # you wanna close market everything at the last timestamp so that you are not left with any upnl
            if i == self.L - 1:
                self.order_type = 'market'
                self.target_quantities_long = [0] * self.np
                self.target_quantities_short = [0] * self.np
                ordermanager = Order_Manager(self.position_mode,
                                             self.order_type,
                                             self.np,
                                             self.ticksizes
                                             )
                ordermanager.reinit_all_pendings()
            else:
                pass
            # then return the list of pending orders given order type (if market or limit) ; if limit, given the local delta high/low
            new_orders = ordermanager.get_new_orders(i,
                                                     self.opens[i, :],
                                                     self.target_quantities_long,
                                                     self.target_quantities_short,
                                                     self.current_quantities,
                                                     self.local_delta_high,
                                                     self.local_delta_low
                                                     )

            ordermanager.update_pending(new_orders)  # format pending [delta_qty, side, targetprice, order_type]

            # given limit order list, (or market orders), return filled orders
            if self.position_mode == 'directional':
                filled_orders = ordermanager.get_filled_orders(self.lows[i, :], self.highs[i, :])
                self.rpnl += self.execute_orders(filled_orders)
            else: 
                raise NotImplementedError
                # filled_orders_long_channel, filled_orders_short_channel = ordermanager.get_filled_orders(self.lows[i, :],self.highs[i, :])
                # etc
            # todo we could win some serious exec time by precomputing filled orders with numpy (ie. outside the loop)
            self.update_balance(i)

    def update_balance(self, i):
        self.upnl = 0
        for k in range(self.np):
            if self.current_quantities[k] == 0:
                pass
            elif self.current_quantities[k] > 0:
                self.upnl += (self.closes[i, k] - self.previous_entry_price[k]) * self.current_quantities[k]
            else:
                # formula for shorts abs(qty)*(entry_price - now_price), equivalent to:
                self.upnl += self.current_quantities[k] * (self.closes[i, k] - self.previous_entry_price[k])

        self.wb.append(self.wb[-1] + self.rpnl)  # wallet balance += rpnl
        self.margin.append(self.wb[-1] + self.upnl)  # equity += rpnl + upnl

    def run(self):
        self.run_simu()
        compute = Stats(self.drawdown_window, self.margin)
        dd, fr = compute.return_stats()
        buy_n_hold = self.opens[-1] / self.opens[0]
        return self.margin, dd, fr, buy_n_hold

    def enterbuy(self, k, exec_price, quantity, fee):
        rpnl = - quantity * exec_price * fee  # quantity >0
        self.current_quantities[k] = quantity
        self.previous_entry_price[k] = exec_price
        return rpnl

    def addbuy(self, k, exec_price, previous_qty, target_quantity, fee):
        delta_qty = target_quantity - previous_qty # >0
        rpnl = - delta_qty * exec_price * fee  # delta qty is > 0 here
        # this redefines the avg entry price as:
        self.previous_entry_price[k] = (self.previous_entry_price[k] * previous_qty + exec_price * delta_qty) / target_quantity
        self.current_quantities[k] = target_quantity
        return rpnl

    def partialclosebuy(self, k, exec_price, previous_qty, target_quantity, fee):
        '''partial close a buy position : avg entry price doesnt change ; standard formula for CFD contracts'''
        delta_qty =  target_quantity - previous_qty # <0
        rpnl = delta_qty * exec_price * fee  # recall that deltaqty < 0 here
        rpnl -= delta_qty * (exec_price - self.previous_entry_price[k])  # recall that deltaqty < 0 here
        self.current_quantities[k] = target_quantity
        return rpnl

    def closebuy(self, k, exec_price, previous_qty, fee):
        delta_qty = - previous_qty # < 0 because target_qty allways 0 here
        rpnl = delta_qty * exec_price * fee  # recall that deltaqty < 0 here
        rpnl -= delta_qty * (exec_price - self.previous_entry_price[k])  # recall that deltaqty < 0 here
        self.current_quantities[k] = 0
        self.previous_entry_price[k] = None
        return rpnl

    def entershort(self, k, exec_price, new_qty, fee):
        rpnl = new_qty * exec_price * fee  # new qty < 0
        self.current_quantities[k] = new_qty
        self.previous_entry_price[k] = exec_price
        return rpnl

    def addshort(self, k, exec_price, previous_qty, target_quantity, fee):
        delta_qty = target_quantity - previous_qty #negative here since tqty < pqty < 0
        rpnl = delta_qty * exec_price * fee  # deltaqty < 0 here
        # signs ok here since the 3 qtities are all <0:
        self.previous_entry_price[k] = (self.previous_entry_price[k] * previous_qty + exec_price * delta_qty) / target_quantity
        self.current_quantities[k] = target_quantity
        return rpnl

    def partialcloseshort(self, k, exec_price, previous_qty, target_quantity, fee):
        delta_qty = target_quantity - previous_qty # positive
        rpnl = - delta_qty * exec_price * fee  # deltaqty is positive
        # CFD rule of calculation for short : rpnl = abs(qty)*(entryprice - execprice), hence
        rpnl += delta_qty * (self.previous_entry_price[k] - exec_price)
        self.current_quantities[k] = target_quantity
        return rpnl

    def closeshort(self, k, exec_price, previous_qty, fee):
        delta_qty = - previous_qty #positive ; target is zero
        rpnl = - delta_qty * exec_price * fee  # deltaqty > 0
        rpnl += delta_qty * (self.previous_entry_price[k] - exec_price)
        self.current_quantities[k] = 0
        self.previous_entry_price[k] = None
        return rpnl

    def execute_orders(self, filled_orders):
        ''' 'execute' orders meaning compute rpnl and update avg entry price and current quantities '''
        # format is [i, k, qty, 'side', targetprice, 'limit'] #traget_price is also the exec_price
        rpnl = 0
        for order in filled_orders:
            k = order[1]
            delta_qty = order[2]
            exec_price = order[4]
            order_type = order[5]
            fee = self.fee_limit if order_type == 'limit' else self.fee_market
            previous_qty = self.current_quantities[k]
            target_quantity = previous_qty + delta_qty

            # you can probably concatenate these functions in only a few of them but its clearer this way
            if previous_qty > 0:
                if delta_qty > 0:
                    rpnl += self.addbuy(k, exec_price, previous_qty, target_quantity, fee)
                elif delta_qty < 0 and target_quantity > 0:
                    rpnl += self.partialclosebuy(k, exec_price, previous_qty, target_quantity, fee)
                elif target_quantity == 0:
                    rpnl += self.closebuy(k, exec_price, previous_qty, fee)
                elif target_quantity < 0:
                    rpnl += self.closebuy(k, exec_price, previous_qty, fee)
                    rpnl += self.entershort(k, exec_price, target_quantity, fee)
                else:
                    raise ValueError

            elif previous_qty == 0:
                if target_quantity > 0:
                    rpnl += self.enterbuy(k, exec_price, target_quantity, fee)
                elif target_quantity < 0:
                    rpnl += self.entershort(k, exec_price, target_quantity, fee)
                else:
                    raise ValueError

            else:  # previous_qty < 0
                if delta_qty < 0:
                    rpnl += self.addshort(k, exec_price, previous_qty, target_quantity, fee)
                elif delta_qty > 0 and target_quantity < 0:
                    rpnl += self.partialcloseshort(k, exec_price, previous_qty, target_quantity, fee)
                elif abs(target_quantity) < 1e-10: #weird numerical precision errors sometimes
                    rpnl += self.closeshort(k, exec_price, previous_qty, fee)
                elif target_quantity > 0:
                    rpnl += self.closeshort( k, exec_price, previous_qty, fee)
                    rpnl += self.enterbuy(k, exec_price, target_quantity, fee)
                else:
                    print(delta_qty, target_quantity)
                    raise ValueError

        return rpnl


class Order_Manager():

    def __init__(self, position_mode, order_type, np, ticksizes):
        self.position_mode = position_mode
        self.np = np
        self.order_type = order_type
        self.ticksizes = ticksizes

    def reinit_all_pendings(self):
        self.pending_entry_orders = [[] for _ in range(self.np)]

    def get_new_orders(self, i, localopens, target_quantities_long, target_quantities_short,
                       current_quantities, local_delta_high, local_delta_low):

        new_orders = []

        for k in range(self.np):
            dh = local_delta_high[k]
            dl = local_delta_low[k]
            if self.position_mode == 'hedge':
                raise NotImplementedError
            else:
                target_quantity = target_quantities_long[k] - target_quantities_short[k]
                delta_qty = target_quantity - current_quantities[k]

                if delta_qty > 0:
                    if self.order_type == 'market':
                        currentprice = localopens[k]
                        targetprice = currentprice + self.ticksizes[k]
                        new_orders.append([i, k, delta_qty, 'buy', targetprice, 'market'])
                    else:
                        n = len(dl)
                        for u in range(n):
                            targetprice = dl[u]
                            new_orders.append([i, k, delta_qty / n, 'buy', targetprice, 'limit'])

                elif delta_qty == 0:
                    pass

                else:
                    if self.order_type == 'market':
                        currentprice = localopens[k]
                        targetprice = currentprice - self.ticksizes[k]
                        new_orders.append([i, k, delta_qty, 'sell', targetprice, 'market'])
                    else:
                        n = len(dh)
                        for u in range(n):
                            targetprice = dh[u]
                            new_orders.append([i, k, delta_qty / n, 'sell', targetprice, 'limit'])

        return new_orders

    def update_pending(self, neworders):
        '''updates the pending list with new orders. Not necessary to maintain a pending list if
        all orders are set at the beginning of the candle, but required if not (not implemented here)'''
        # format: [i, k, qty, 'side', targetprice, 'limit']
        for elem in neworders:
            k = elem[1]
            self.pending_entry_orders[k].append(elem)

    def get_filled_orders(self, local_lows, local_highs):
        filled = []
        for k in range(self.np):
            pending_k = self.pending_entry_orders[k]
            # format of pending : [i, k, qty, 'side', targetprice, 'limit']
            if len(pending_k):
                for j in range(len(pending_k)):  # handling multiple orders for same symbol k
                    targetprice = pending_k[j][4]
                    ordertype = pending_k[j][5]
                    side = pending_k[j][3]
                    if ordertype == 'market':  # allways filled
                        filled.append(pending_k[j])
                    else:
                        if side == 'buy':
                            # the exec price is the target_price ok; but for this the low must be below it by at least one tick:
                            if local_lows[k] <= targetprice - self.ticksizes[k]:
                                filled.append(pending_k[j])
                        if side == 'sell':
                            if local_highs[k] >= targetprice + self.ticksizes[k]:
                                filled.append(pending_k[j])
        return filled


class Stats():
    '''additional stats can be implemented here, sharpe ratio, return volatility etc'''

    def __init__(self, window, margin):
        self.window = window
        self.margin = margin

    def compute_drawdown(self, margin):
        '''loose way of approximating the DD ; exact caluclation here would be O(L^3) which is too expensive
        Probably some DP solutions to make it faster ; todo'''
        drawdowns = []
        h = self.window // 4
        L = len(margin)
        for i in range(0, L, h):
            submargin = margin[i:i + h]
            step = h // 10
            if step < 1: step = 1
            for j in range(1, len(submargin), step):  # step 10 less precise but much faster
                m = min(submargin[j:])
                M = max(submargin[:j])
                if M == 0:
                    drawdowns.append(-1)
                else:
                    drawdowns.append((m - M) / M)
        return 100 * min(drawdowns)  # in percents ; -100 if broke

    def return_stats(self):
        if self.margin[-1] == 0:
            drawdown, final_return = -100, -100  # the last one shd be 0 but this way we overpenalize very bad startegies in learning
            return drawdown, final_return
        else:
            drawdown = self.compute_drawdown(self.margin)
            final_return = self.margin[-1] / self.margin[0]  # instead of percents
            return drawdown, final_return
