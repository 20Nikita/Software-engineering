#include "user_service/http_user_server.h"


int main(int argc, char*argv[]) 
{
    HTTPUserServer app;
    return app.run(argc, argv);
}