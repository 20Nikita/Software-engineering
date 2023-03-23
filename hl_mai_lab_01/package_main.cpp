
#include <optional>
#include "package_service/http_package_server.h"

int main(int argc, char*argv[]) 
{
    HTTPPackageServer app;
    return app.run(argc, argv);
}