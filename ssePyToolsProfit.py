"""
@author:        Wilson Chavez
@create:        2025-07-30
@description:   Finance-Focused functions using a server-side extension (SSE) for Qlik Sense built using Python.
@version:       1.0
@license:       MIT
"""

import grpc
import logging
import time
import json
import platform
import pandas as pd
import numpy as np
import multiprocessing
from concurrent import futures
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant
from scipy import stats

import ServerSideExtension_pb2 as SSE
import ServerSideExtension_pb2_grpc as SSE_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if platform.system() == "Windows":
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        pass

class ProfitabilityAnalysisSSE(SSE_grpc.ConnectorServicer):
    def __init__(self):
        self.functions = {
            0: self._margin_volatility_analysis,
            1: self._break_even_analysis,
            2: self._price_elasticity_analysis
        }
    
    def GetCapabilities(self, request, context):
        try:
            # Define functions available to QlikSense
            functions = [
                SSE.FunctionDefinition(
                    name='MarginVolatility',
                    functionId=0,
                    functionType=SSE.AGGREGATION,
                    returnType=SSE.DUAL,
                    params=[
                        SSE.Parameter(name='Sales', dataType=SSE.NUMERIC),
                        SSE.Parameter(name='Costs', dataType=SSE.NUMERIC)
                    ]
                ),
                SSE.FunctionDefinition(
                    name='BreakEvenAnalysis',
                    functionId=1,
                    functionType=SSE.AGGREGATION,
                    returnType=SSE.DUAL,
                    params=[
                        SSE.Parameter(name='FixedCosts', dataType=SSE.NUMERIC),
                        SSE.Parameter(name='VariableCostPerUnit', dataType=SSE.NUMERIC),
                        SSE.Parameter(name='PricePerUnit', dataType=SSE.NUMERIC),
                        SSE.Parameter(name='UnitsSold', dataType=SSE.NUMERIC)
                    ]
                ),
                SSE.FunctionDefinition(
                    name='PriceElasticity',
                    functionId=2,
                    functionType=SSE.AGGREGATION,
                    returnType=SSE.DUAL,
                    params=[
                        SSE.Parameter(name='PricePerUnit', dataType=SSE.NUMERIC),
                        SSE.Parameter(name='UnitsSold', dataType=SSE.NUMERIC),
                        SSE.Parameter(name='BackOrder', dataType=SSE.NUMERIC),
                        SSE.Parameter(name='Month', dataType=SSE.NUMERIC)
                    ]
                )
            ]
            
            capabilities = SSE.Capabilities(
                pluginIdentifier = "PyTools Profit",
                pluginVersion = "1.0",
                allowScript=False,
                functions=functions
            )
        
        except Exception as e:
            logger.error(f"Error {type(e)} in GetCapabilities: {e}")
            
        logger.info("Capabilities sent to QlikSense")
        return capabilities
    
    def ExecuteFunction(self, request_iterator, context):
        try:            
            # Get function ID from gRPC metadata
            metadata = dict(context.invocation_metadata())
            function_id = None
            if 'qlik-functionid' in metadata:
                function_id = int(metadata['qlik-functionid'])
            elif 'qlik-function-id' in metadata:
                function_id = int(metadata['qlik-function-id'])
                
            # Decode binary metadata for function request header
            if 'qlik-functionrequestheader-bin' in metadata:
                binary_header = metadata['qlik-functionrequestheader-bin']
                
                try:
                    # Parse as protobuf FunctionRequestHeader
                    func_header = SSE.FunctionRequestHeader()
                    func_header.ParseFromString(binary_header)
                    function_id = func_header.functionId
                    logger.info(f"Function ID: {function_id}")
                    
                except Exception as parse_error:
                    logger.error(f"Error parsing binary header: {parse_error}")
                    # Fallback: manual parsing
                    if len(binary_header) >= 2 and binary_header[0] == 0x08:
                        function_id = binary_header[1]
                        logger.info(f"Function ID (manual): {function_id}")
            
            if function_id is None:
                logger.error("Could not determine function ID")
                yield SSE.BundledRows(rows=[
                    SSE.Row(duals=[SSE.Dual(numData=0, strData="No Function ID")])
                ])
                return
                
            logger.info(f"Executing function ID: {function_id}")
            
            # Collect all data rows
            rows = []
            for request in request_iterator:
                for row in request.rows:
                    rows.append(row)
            
            # Execute the appropriate function
            if function_id in self.functions:
                result = self.functions[function_id](rows)
                yield result
            else:
                logger.error(f"Unknown function ID: {function_id}")
                yield SSE.BundledRows(rows=[
                    SSE.Row(duals=[SSE.Dual(numData=0, strData="Unknown Function")])
                ])
            
        except Exception as e:
            logger.error(f"Error {type(e)} in ExecuteFunction: {e}")
            
            yield SSE.BundledRows(rows=[
                SSE.Row(duals=[SSE.Dual(numData=0, strData=f"Error: {str(e)}")])
            ])
    
    def _margin_volatility_analysis(self, rows):
        try:
            margins = []
            for row in rows:
                sales = row.duals[0].numData
                costs = row.duals[1].numData
                margin = (sales - costs) / sales if sales > 0 else 0
                margins.append(margin)
            
            margins = np.array(margins)
            
            # Calculate volatility metrics
            volatility = np.std(margins)
            mean_margin = np.mean(margins)
            
            # Risk assessment
            cv = volatility / mean_margin if mean_margin != 0 else 0
            if cv < 0.1:
                risk_level = "Low Risk"
            elif cv < 0.3:
                risk_level = "Moderate Risk"
            else:
                risk_level = "High Risk"
            
            return SSE.BundledRows(rows=[
                SSE.Row(duals=[
                    SSE.Dual(numData=volatility, strData=risk_level)
                ])
            ])
            
        except Exception as e:
            logger.error(f"Error {type(e)} in _margin_volatility_analysis: {e}")
            
            return SSE.BundledRows(rows=[
                SSE.Row(duals=[SSE.Dual(numData=0, strData=f"Error: {str(e)}")])
            ])
    
    def _break_even_analysis(self, rows):
        try:
            row = rows[0]
            fixed_costs = row.duals[0].numData
            variable_cost_per_unit = row.duals[1].numData
            price_per_unit = row.duals[2].numData
            units_sold = row.duals[3].numData
            
            # Break-even calculation
            contribution_margin = price_per_unit - variable_cost_per_unit
            break_even_units = fixed_costs / contribution_margin if contribution_margin > 0 else 0
            
            # Safety analysis
            margin_of_safety_pct=0
            if units_sold>0:
                margin_of_safety_pct = ((units_sold - break_even_units) / units_sold)
            
            status = f"BE Units: {break_even_units:.0f}, BE Margin Safety: {margin_of_safety_pct:.3f}"
            
            return SSE.BundledRows(rows=[
                SSE.Row(duals=[
                    SSE.Dual(numData=break_even_units, strData=status)
                ])
            ])
            
        except Exception as e:
            logger.error(f"Error {type(e)} in _break_even_analysis: {e}")
            
            return SSE.BundledRows(rows=[
                SSE.Row(duals=[SSE.Dual(numData=0, strData=f"Error: {str(e)}")])
            ])
    
    def _price_elasticity_analysis(self, rows):
        try:
            # Validate sufficient data
            if len(rows) < 2:
                analysis = {
                    "elasticity": 0,
                    "r_squared": 0,
                    "interpretation": "Insufficient Data",
                    "classification": "No Data"
                }
                return SSE.BundledRows(rows=[
                    SSE.Row(duals=[
                        SSE.Dual(strData=json.dumps(analysis))
                    ])
                ])
            
            data = []
            for row in rows:
                price_per_unit = row.duals[0].numData
                units_sold = row.duals[1].numData
                backorder_units = row.duals[2].numData
                month = row.duals[3].numData
                
                quantity = units_sold + backorder_units
                # Validate valid data
                if price_per_unit > 0 and quantity > 0:
                    # Use logarithms to calculate
                    log_price = np.log(price_per_unit)
                    log_quantity = np.log(quantity)
                    data.append([month, log_price, log_quantity])
            
            # Verify valid data after apply filter
            if len(data) < 2:
                analysis = {
                    "elasticity": 0,
                    "r_squared": 0,
                    "interpretation": "Invalid Data",
                    "classification": "No Valid Data"
                }
                return SSE.BundledRows(rows=[
                    SSE.Row(duals=[
                        SSE.Dual(strData=json.dumps(analysis))
                    ])
                ])
            
            df = pd.DataFrame(data, columns=['Month','LogPrice', 'LogQuantity'])
            df = df.sort_values('Month')
            
            # Check for variation in prices
            if df['LogPrice'].nunique() < 2:
                analysis = {
                    "elasticity": 0,
                    "r_squared": 0,
                    "interpretation": "No Price Variation",
                    "classification": "Constant Price"
                }
                return SSE.BundledRows(rows=[
                    SSE.Row(duals=[
                        SSE.Dual(strData=json.dumps(analysis))
                    ])
                ])
                
            # Calculate the price elasticity of demand using linear regression
            X = add_constant(df['LogPrice'])
            model = OLS(df['LogQuantity'], X).fit()
            
            # Price elasticity of demand
            elasticity = model.params['LogPrice']
            # Coefficient of determination (R²)
            r_squared = model.rsquared
            
            # Classify and Interpretation of the elasticity
            if abs(elasticity) > 1:
                classification = "Elastic"
                if elasticity < -1:
                    interpretation = "Highly Price Sensitive (Elastic)"
                else:
                    interpretation = "Unusual Positive Elasticity"
            elif abs(elasticity) == 1:
                classification = "Unit Elastic"
                interpretation = "Unit Elastic"
            elif abs(elasticity) < 1 and abs(elasticity) > 0:
                classification = "Inelastic"
                interpretation = "Low Price Sensitivity (Inelastic)"
            else:
                classification = "Perfectly Inelastic"
                interpretation = "No Price Sensitivity"
            
            # Check if the elasticity shows unusual behavior
            if elasticity > 0:
                interpretation += " - Warning: Positive elasticity (unusual)"
            
            analysis = {
                "elasticity": round(elasticity, 3),
                "r_squared": round(r_squared, 3),
                "interpretation": interpretation,
                "classification": classification,
                "sample_size": len(data)
            }
            
            return SSE.BundledRows(rows=[
                SSE.Row(duals=[
                    SSE.Dual(strData=json.dumps(analysis))
                ])
            ])
            
        except Exception as e:
            logger.error(f"Error {type(e)} in _price_elasticity_analysis: {e}")
            
            dataError = {
                "elasticity": 0,
                "r_squared": 0,
                "interpretation": f"Error: {str(e)}",
                "classification": "Error"
            }
            return SSE.BundledRows(rows=[
                SSE.Row(duals=[
                    SSE.Dual(strData=json.dumps(dataError))
                ])
            ])
            
def main():
    num_cpus = multiprocessing.cpu_count()
    num_workers = max(1, min(num_cpus - 1, 8))
    
    # Create server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_workers))
    SSE_grpc.add_ConnectorServicer_to_server(ProfitabilityAnalysisSSE(), server)
    
    # Listen on port 50052
    port = '50052'
    server.add_insecure_port(f'[::]:{port}')
    
    # Start server
    server.start()
    logger.info(f"Server SSE running on port {port}")
    
    try:
        while True:
            time.sleep(86400)  # Keep server running
    except KeyboardInterrupt:
        logger.info("Stopping server...")
        server.stop(0)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()