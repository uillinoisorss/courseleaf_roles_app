import logging

import azure.functions as func

import shared.courseleaf_functions as cf

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# http://localhost:7071/admin/functions/courseleaf_roles_app_data_load
# curl --request POST -H "Content-Type:application/json" --data '{"input":"null"}' http://localhost:7071/admin/functions/courseleaf_roles_app_data_load

@app.timer_trigger(schedule="0 0 6 * * *", arg_name = "timer", run_on_startup = False, use_monitor = False) 
def courseleaf_roles_app_data_load(timer: func.TimerRequest) -> None:
    
    if timer.past_due:
        logging.info('COURSELEAF_CONTACTS: The timer is past due!')

    cf.execute_data_load()


@app.route(route="courseleaf_data_load_test", auth_level=func.AuthLevel.ANONYMOUS)
def courseleaf_data_load_test(req: func.HttpRequest) -> func.HttpResponse:
    try:
        cf.execute_data_load()
        return func.HttpResponse(f"Test data load completed successfully.")
    except Exception as e:
        return func.HttpResponse(f"Something went wrong! {print(e)}")
