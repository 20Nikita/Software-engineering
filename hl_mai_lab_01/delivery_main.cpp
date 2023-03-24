
#include <optional>
#include "delivery_service/http_delivery_server.h"

int main(int argc, char*argv[]) 
{
    HTTPDeliveryServer app;
    return app.run(argc, argv);
}