import logging
import os

import azure.functions as func

import shared.courseleaf_functions as cf
import shared.etl_functions as etl

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.timer_trigger(schedule="0 0 6 * * *", arg_name = "timer", run_on_startup = False, use_monitor = False) 
def courseleaf_roles_app_data_load(timer: func.TimerRequest) -> None:
    
    if timer.past_due:
        logging.info('COURSELEAF_CONTACTS: The timer is past due!')

    cf.execute_data_load()


@app.route(route="courseleaf_data_load_test", auth_level=func.AuthLevel.ANONYMOUS)
def courseleaf_data_load_test(req: func.HttpRequest) -> func.HttpResponse:
    try:
        cf.execute_data_load()
        return func.HttpResponse(f"COURSELEAF_CONTACTS: Test data load completed successfully.")
    except Exception as e:
        return func.HttpResponse(f"COURSELEAF_CONTACTS: Something went wrong! {print(e)}")


@app.route(route="courseleaf_debug", auth_level=func.AuthLevel.ANONYMOUS)
def courseleaf_debug(req: func.HttpRequest) -> func.HttpResponse:
    try:
        cf.execute_data_load()
        return func.HttpResponse(f"COURSELEAF_DEBUG: Success!")
    except Exception as e:
        return func.HttpResponse(f"COURSELEAF_DEBUG: Something went wrong! Error details: {print(e)}")
