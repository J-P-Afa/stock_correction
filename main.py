from database_utils.database_browser import DatabaseBrowser
from datetime import datetime, timedelta
from pandas import DataFrame, notnull, read_excel, isna, merge


START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 7, 3)
STOCK_MOVEMENTS_QUERY = f"""
select 
    stock_movement.ES02_ID as movement_id,
    stock_movement.ES02_DATA as movement_date,
    stock_movement.OR01_ID as production_order_id,
    purchase.RM01_DATA_ENTRADA as entry_date,
    stock_movement.ES02_DOCUMENTO as document_number,
    stock_movement.ES02_HISTORICO as movement_history,
    item.CD04_ID as item_id,
    item.CD04_DESCRICAO as item_description,
    stock_movement.ES02_QTDE as quantity,
    stock_movement.ES02_CMV_MEDIO as average_cost,
    stock_movement.ES02_CMV_TOTAL as total_cost
from TB_ES_02 as stock_movement
inner join TB_ES_01 as stock on 
    stock_movement.ES01_ID = stock.ES01_ID
inner join TB_CD_04 as item on 
    item.CD04_ID = stock.CD04_ID
inner join TB_CD_05 as item_use on 
    item_use.CD04_ID = item.CD04_ID
    and item_use.SY01_ID = 3
    and item_use.CD05_USO_EMPRESA not in (7, 10, 11)
inner join TB_CD_28 as unity_measure on 
    item.CD28_ID = unity_measure.CD28_ID
inner join TB_CD_02 as ncm on 
    item.CD02_ID = ncm.CD02_ID
left join TB_RM_01 as purchase on
    purchase.RM01_ID = stock_movement.RM01_ID
where 
    (
        stock_movement.ES02_DATA between '{START_DATE.strftime('%d/%m/%Y')}' and '{END_DATE.strftime('%d/%m/%Y')}'
        or stock_movement.OR01_ID in (89710)
    )
    and stock.SY01_ID = 3
    and not (stock_movement.ES02_HISTORICO like '%TRANSF%ALMOX%' or stock_movement.ES02_HISTORICO = 'T')
"""


class StockResume(DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    @classmethod
    def new_empty(cls) -> 'StockResume':
        return cls({
            'date': [],
            'corrected_cost': [],
            'original_cost': []
        })
    

    def get_last_day_of_month(self, month: int, year: int) -> datetime:
        if month == 12:
            return datetime(year, month, 31)   
        return datetime(year, month + 1, 1) - timedelta(days=1)


    def insert_new_entry(self, month: int, year: int, corrected_cost: float, original_cost: float) -> None:
        self.loc[len(self)] = { # type: ignore
            'date': self.get_last_day_of_month(month, year),
            'corrected_cost': corrected_cost,
            'original_cost': original_cost 
        }


class Stock(DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_item_id_as_index()
        self.set_average_cost()


    def item_id_exists(self, item_id: int) -> bool:
        return item_id in self.index


    def set_item_id_as_index(self) -> None:
        self.set_index('item_id', inplace=True)


    def set_average_cost(self) -> None:
        self['average_cost'] = self.apply(
            lambda row: row['total_cost'] / row['quantity'] if row['quantity'] != 0 else 0.0, 
            axis=1
        )


    def insert_new_item_with_empty_stock(self, item_id: int, description: str) -> None:
        if not self.item_id_exists(item_id):
            self.loc[item_id] = {  # type: ignore
                'description': description,
                'quantity': 0.0,
                'average_cost': 0.0,
                'total_cost': 0.0,
                'original_total_cost': 0.0
            }
        else:
            raise ValueError(f"Item {item_id} already exists in stock.")


    def get_stock(self, item_id: int) -> DataFrame:
        if not self.item_id_exists(item_id):
            raise ValueError(f"Item {item_id} not found in stock.")
        return self.loc[[item_id]]  


    def insert_transaction(self, item_id: int, quantity: float, cost: float, original_cost: float) -> None:
        if not self.item_id_exists(item_id):
            raise ValueError(f"Item {item_id} not found in stock.")
        self.at[item_id, 'quantity'] += quantity
        self.at[item_id, 'total_cost'] += cost
        if isna(self.at[item_id, 'total_cost']):
            pass
        self.at[item_id, 'original_total_cost'] += original_cost
        self.at[item_id, 'average_cost'] = (
            self.at[item_id, 'total_cost'] / self.at[item_id, 'quantity']
            if self.at[item_id, 'quantity'] != 0 else 0.0
        )


class StockMovements(DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_init_columns()
        self.order_by_correct_movement_date()
        self.set_dismantling_id()
        self.set_movement_cost_is_already_correct()
        self.correct_column_types()


    def correct_column_types(self) -> None:
        self['production_order_id'] = self['production_order_id'].astype('Int64')


    def set_init_columns(self) -> None:
        self.set_movement_id_as_index()
        self.set_correct_movement_date()
        self.set_is_dismantling()
        self.set_is_dismantling_input()
        self.set_is_dismantling_output()
        self.set_is_production_order()
        self.set_is_production_order_input()
        self.set_is_production_order_output()
        self.set_is_entry()


    def set_movement_id_as_index(self) -> None:
        self.set_index('movement_id', inplace=True)


    def set_correct_movement_date(self) -> None:
        self['correct_movement_date'] = self.apply(
            lambda row: row['entry_date'] if notnull(row['entry_date']) else row['movement_date'], 
            axis=1
        )


    def set_is_dismantling(self) -> None:
        self['is_dismantling'] = self.document_number.str.contains('DESMONTE', case=False)


    def set_is_dismantling_input(self) -> None:
        self['is_dismantling_input'] = (
            (self.total_cost < 0) & self.is_dismantling
        )

    
    def set_is_dismantling_output(self) -> None:
        self['is_dismantling_output'] = (
            (self.total_cost > 0) & self.is_dismantling
        )


    def set_is_production_order(self) -> None:
        self['is_production_order'] = self.production_order_id.notnull()
            

    def set_is_production_order_input(self) -> None:
        self['is_production_order_input'] = (
            self.movement_history.str.contains('REQUISICAO') & self.is_production_order
        )

    
    def set_is_production_order_output(self) -> None:
        self['is_production_order_output'] = (
            # (self.total_cost > 0) & self.is_production_order
            self.movement_history.str.contains('ENC') & self.movement_history.str.contains('ORDEM') & self.is_production_order
        )


    def order_by_correct_movement_date(self) -> None:
        self.sort_values(by=['correct_movement_date', 'movement_id'], inplace=True)


    def set_dismantling_id(self) -> None:
        dismantlings = self[self.is_dismantling]
        dismantlings['dismantling_id'] = None
        dismantlings['dismantling_id'] = dismantlings.dismantling_id.astype('Int64')
        dismantling_id = None
        last_was_output = False
        for index, row in dismantlings.iterrows():
            if dismantling_id is None or (row.is_dismantling_input and last_was_output):
                dismantling_id = index
                last_was_output = False
            if row.is_dismantling_output:
                last_was_output = True
            self.at[index, 'dismantling_id'] = dismantling_id
            

    def set_is_entry(self) -> None:
        self['is_entry'] = self.movement_history.str.contains('RECEBIMENTO')
    

    def set_movement_cost_is_already_correct(self) -> None:
        self['original_cost'] = self.total_cost
        self['total_cost'] = self.apply(
            lambda row: row['correct_cost'] if notnull(row['correct_cost']) else row['total_cost'], 
            axis=1
        )
        self['movement_cost_is_already_correct'] = (
            ~isna(self.correct_cost) | self.is_entry
        )


    def correct_dismantling(self, dismantling_id: int, stock: Stock) -> None:
        dismantling = self[self.dismantling_id == dismantling_id]
        if dismantling.empty:
            raise ValueError(f"No dismantling found with ID {dismantling_id}.")
        
        input_movements = dismantling[dismantling.is_dismantling_input]

        output_movements = dismantling[dismantling.is_dismantling_output]
        outputs_original_cost = output_movements.original_cost.sum()

        if input_movements.empty or output_movements.empty:
            raise ValueError(f"Dismantling must have both input and output movements: ID {dismantling_id}.")

        inputs_corrected_cost = 0.0
        for index, row in input_movements.iterrows():
            avg_stock = stock.get_stock(row.item_id).average_cost.iloc[0]
            # row['total_cost'] = row.quantity * avg_stock
            # row['average_cost'] = avg_stock
            # row['movement_cost_is_already_correct'] = True
            self.at[index, 'total_cost'] = row.quantity * avg_stock
            self.at[index, 'average_cost'] = avg_stock
            self.at[index, 'movement_cost_is_already_correct'] = True
            inputs_corrected_cost += (row.quantity * avg_stock) * -1

        for index, row in output_movements.iterrows():
            participation = row.original_cost / outputs_original_cost
            # row['total_cost'] = inputs_corrected_cost * participation
            # row['average_cost'] = row.total_cost / row.quantity
            # row['movement_cost_is_already_correct'] = True
            self.at[index, 'total_cost'] = inputs_corrected_cost * participation
            self.at[index, 'average_cost'] = self.loc[index, 'total_cost'] / row.quantity
            self.at[index, 'movement_cost_is_already_correct'] = True
        pass
            

    def correct_production_order(self, production_order_id: int, stock: Stock) -> None:
        production_order = self[self.production_order_id == production_order_id]
        if production_order.empty:
            raise ValueError(f"No production order found with ID {production_order_id}.")
        
        input_movements = production_order[production_order.is_production_order_input]
        output_movements = production_order[production_order.is_production_order_output]

        if input_movements.empty: # or output_movements.empty:
            raise ValueError(f"Production order must have both input and output movements: ID {production_order_id}.")

        inputs_corrected_cost = 0.0
        for index, row in input_movements.iterrows():
            avg_stock = stock.get_stock(row.item_id).average_cost.iloc[0]
            # row['total_cost'] = row.quantity * avg_stock
            # row['average_cost'] = avg_stock
            # row['movement_cost_is_already_correct'] = True
            self.at[index, 'total_cost'] = row.quantity * avg_stock
            self.at[index, 'average_cost'] = avg_stock
            self.at[index, 'movement_cost_is_already_correct'] = True
            inputs_corrected_cost += (row.quantity * avg_stock) * -1
            
        for index, row in output_movements.iterrows():
            participation = row.original_cost / output_movements.original_cost.sum()
            # row['total_cost'] = inputs_corrected_cost * participation
            # row['average_cost'] = row.total_cost / row.quantity
            # row['movement_cost_is_already_correct'] = True
            self.at[index, 'total_cost'] = inputs_corrected_cost * participation
            if isna(self.loc[index, 'total_cost']):
                pass
            self.at[index, 'average_cost'] = self.loc[index, 'total_cost'] / row.quantity
            self.at[index, 'movement_cost_is_already_correct'] = True


    def correct_movement_cost_by_stock(self, movement_id: int, stock: Stock) -> None:
        movement = self.loc[movement_id]
        if movement.movement_cost_is_already_correct:
            raise ValueError(f"Movement {movement_id} already has the correct cost.")
        avg_stock = stock.get_stock(movement.item_id).average_cost.iloc[0] # type: ignore
        self.at[movement_id, 'total_cost'] = avg_stock * movement.quantity 
        self.at[movement_id, 'average_cost'] = avg_stock
        self.at[movement_id, 'movement_cost_is_already_correct'] = True
        pass


    def insert_movement_to_stock(self, movement_id: int, stock: Stock) -> None:
        movement = self.loc[movement_id]
        if not stock.item_id_exists(movement.item_id): # type: ignore
            stock.insert_new_item_with_empty_stock(
                item_id=movement.item_id, # type: ignore
                description=movement.item_description # type: ignore
            )

        delta = movement.original_cost - movement.total_cost
        delta_p = abs(delta / movement.original_cost)
        if delta_p > 10.00 and 'INVENT' in movement.movement_history:
            self.at[movement_id, 'total_cost'] = movement.original_cost

        if isna(movement.total_cost):
            pass
        
        stock.insert_transaction(
            item_id=movement.item_id, # type: ignore
            quantity=movement.quantity, # type: ignore
            cost=movement.total_cost, # type: ignore
            original_cost=movement.original_cost # type: ignore
        )


    def correct_costs_and_generate_stocks(self, start_stock: Stock, stock_resume: StockResume) -> None:
        last_date: datetime = None # type: ignore
        for index, row in self.iterrows():
            if last_date is not None and (
                last_date.month != row.correct_movement_date.month or last_date.year != row.correct_movement_date.year
            ):
                stock_resume.insert_new_entry(
                    month=last_date.month, 
                    year=last_date.year, 
                    corrected_cost=start_stock.total_cost.sum(), 
                    original_cost=start_stock.original_total_cost.sum()
                )
                
            last_date = row.correct_movement_date
            
            if row.movement_cost_is_already_correct:
                self.insert_movement_to_stock(index, start_stock) # type: ignore
            else:
                if row.is_dismantling:
                    self.correct_dismantling(row.dismantling_id, start_stock)
                    self.insert_movement_to_stock(index, start_stock) # type: ignore
                    continue
                
                if row.is_production_order:
                    self.correct_production_order(row.production_order_id, start_stock)
                    self.insert_movement_to_stock(index, start_stock) # type: ignore
                    continue
                
                self.correct_movement_cost_by_stock(index, start_stock) # type: ignore
                self.insert_movement_to_stock(index, start_stock) # type: ignore


class CorrectMovementsCosts(DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_movement_id_as_index()

    
    def set_movement_id_as_index(self) -> None:
        self.set_index('movement_id', inplace=True)


def main():
    db_browser = DatabaseBrowser.new_with_jundsoft_connection()
    correct_movements_costs = CorrectMovementsCosts(
        read_excel(
            'correct_movements_costs.xlsx', 
            sheet_name='correct_movements_costs'
        )
    )
    stock_movements = StockMovements(
        merge(
            db_browser.get_query_result(STOCK_MOVEMENTS_QUERY),
            correct_movements_costs, 
            on='movement_id', 
            how='left',
        )
    )
    stock = Stock(
        read_excel(
            'start_stock.xlsx',
            sheet_name='start_stock'
        )
    )
    stock_resume = StockResume.new_empty()

    stock_movements.correct_costs_and_generate_stocks(stock, stock_resume)

    stock_resume.to_excel('analysis/stock_resume.xlsx')
    stock.to_excel('analysis/final_stock.xlsx')
    stock_movements.to_excel('analysis/stock_movements.xlsx')

    pass


if __name__ == '__main__':
    main()
