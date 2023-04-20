#ifndef USEHANDLER_H
#define USEHANDLER_H

#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Exception.h"
#include "Poco/ThreadPool.h"
#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"

#include <iostream>
#include <iostream>
#include <fstream>

using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::ThreadPool;
using Poco::Timestamp;
using Poco::Net::HTMLForm;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPRequestHandlerFactory;
using Poco::Net::HTTPServer;
using Poco::Net::HTTPServerParams;
using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPServerResponse;
using Poco::Net::NameValueCollection;
using Poco::Net::ServerSocket;
using Poco::Util::Application;
using Poco::Util::HelpFormatter;
using Poco::Util::Option;
using Poco::Util::OptionCallback;
using Poco::Util::OptionSet;
using Poco::Util::ServerApplication;

#include <Poco/Base64Encoder.h>
#include "Poco/Net/HTTPSClientSession.h"
#include "Poco/URI.h"

#include "../../database/delivery.h"
#include "../../database/user.h"
#include "../../helper.h"

static bool hasSubstr(const std::string &str, const std::string &substr)
{
    if (str.size() < substr.size())
        return false;
    for (size_t i = 0; i <= str.size() - substr.size(); ++i)
    {
        bool ok{true};
        for (size_t j = 0; ok && (j < substr.size()); ++j)
            ok = (str[i + j] == substr[j]);
        if (ok)
            return true;
    }
    return false;
}

class UserHandler : public HTTPRequestHandler
{
private:
    bool check_name(const std::string &name, std::string &reason)
    {
        if (name.length() < 3)
        {
            reason = "Name must be at leas 3 signs";
            return false;
        }

        if (name.find(' ') != std::string::npos)
        {
            reason = "Name can't contain spaces";
            return false;
        }

        if (name.find('\t') != std::string::npos)
        {
            reason = "Name can't contain spaces";
            return false;
        }

        return true;
    };

public:
    UserHandler(const std::string &format) : _format(format)
    {
    }

    std::optional<std::string> do_get(const std::string &url, const std::string &login, const std::string &password)
    {
        std::string string_result;
        try
        {
            std::string token = login + ":" + password;
            std::ostringstream os;
            Poco::Base64Encoder b64in(os);
            b64in << token;
            b64in.close();
            std::string identity = "Basic " + os.str();

            Poco::URI uri(url);
            Poco::Net::HTTPClientSession s(uri.getHost(), uri.getPort());
            Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.toString());
            request.setVersion(Poco::Net::HTTPMessage::HTTP_1_1);
            request.setContentType("application/json");
            request.set("Authorization", identity);
            request.set("Accept", "application/json");
            request.setKeepAlive(true);

            s.sendRequest(request);

            Poco::Net::HTTPResponse response;
            std::istream &rs = s.receiveResponse(response);

            while (rs)
            {
                char c{};
                rs.read(&c, 1);
                if (rs)
                    string_result += c;
            }
            std :: cout<<17;

            if (response.getStatus() != 200)
                return {};
            std :: cout<<3;
        }
        catch (Poco::Exception &ex)
        {
            std::cout << "exception:" << ex.what() << std::endl;
            return std::optional<std::string>();
        }

        return string_result;
    }

    void handleRequest(HTTPServerRequest &request,
                       HTTPServerResponse &response)
    {
        HTMLForm form(request, request.stream());
        
        try
        {
            std::string scheme;
            std::string info;
            request.getCredentials(scheme, info);
            std::string login, password, url;
            get_identity(info, login, password);
            url = "http://" + host+":8080/auth";
            if (do_get(url, login, password))
            {
                if (hasSubstr(request.getURI(), "/get_state"))
                {
                    long id = atol(form.get("id").c_str());

                    std::optional<database::Delivery> result = database::Delivery::read_by_id(id);
                    if (result)
                    {
                        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                        response.setChunkedTransferEncoding(true);
                        response.setContentType("application/json");
                        std::ostream &ostr = response.send();
                        Poco::JSON::Stringifier::stringify(result->toJSON(), ostr);
                        return;
                    }
                    else
                    {
                        response.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND);
                        response.setChunkedTransferEncoding(true);
                        response.setContentType("application/json");
                        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                        root->set("type", "/errors/not_found");
                        root->set("title", "Internal exception");
                        root->set("status", "404");
                        root->set("detail", "user ot found");
                        root->set("instance", "/user");
                        std::ostream &ostr = response.send();
                        Poco::JSON::Stringifier::stringify(root, ostr);
                        return;
                    }
                }
                else if (hasSubstr(request.getURI(), "/set_state"))
                {
                    long id = atol(form.get("id").c_str());
                    std::string state = form.get("state");

                    std::optional<database::Delivery> result = database::Delivery::set_state(id, state);
                    if (result)
                    {
                        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                        response.setChunkedTransferEncoding(true);
                        response.setContentType("application/json");
                        std::ostream &ostr = response.send();
                        Poco::JSON::Stringifier::stringify(result->toJSON(), ostr);
                        return;
                    }
                    else
                    {
                        response.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND);
                        response.setChunkedTransferEncoding(true);
                        response.setContentType("application/json");
                        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                        root->set("type", "/errors/not_found");
                        root->set("title", "Internal exception");
                        root->set("status", "404");
                        root->set("detail", "user ot found");
                        root->set("instance", "/user");
                        std::ostream &ostr = response.send();
                        Poco::JSON::Stringifier::stringify(root, ostr);
                        return;
                    }
                }
                else if (hasSubstr(request.getURI(), "/read_by_names"))
                // {
                {
                    std::string recipient_name = form.get("recipient_name");
                    std::string sender_name = form.get("sender_name");
                    auto results = database::Delivery::read_by_names(recipient_name, sender_name);
                    Poco::JSON::Array arr;
                    for (auto s : results)
                        arr.add(s.toJSON());
                    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                    response.setChunkedTransferEncoding(true);
                    response.setContentType("application/json");
                    std::ostream &ostr = response.send();
                    Poco::JSON::Stringifier::stringify(arr, ostr);

                    return;
                }    
                //     std::string recipient_name = form.get("recipient_name");
                //     std::string sender_name = form.get("sender_name");

                //     std::optional<database::Delivery> result = database::Delivery::read_by_names(recipient_name, sender_name);
                //     if (result)
                //     {
                //         response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                //         response.setChunkedTransferEncoding(true);
                //         response.setContentType("application/json");
                //         std::ostream &ostr = response.send();
                //         Poco::JSON::Stringifier::stringify(result->toJSON(), ostr);
                //         return;
                //     }
                //     else
                //     {
                //         response.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND);
                //         response.setChunkedTransferEncoding(true);
                //         response.setContentType("application/json");
                //         Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                //         root->set("type", "/errors/not_found");
                //         root->set("title", "Internal exception");
                //         root->set("status", "404");
                //         root->set("detail", "user ot found");
                //         root->set("instance", "/user");
                //         std::ostream &ostr = response.send();
                //         Poco::JSON::Stringifier::stringify(root, ostr);
                //         return;
                //     }
                // }
                else if (hasSubstr(request.getURI(), "/add_delivery") && 
                        form.has("recipient_name") && form.has("sender_name") && form.has("state") && 
                        form.has("sender_addres")  && form.has("date") && form.has("recipient_addres"))
                {
                    database::Delivery user;
                    user.recipient_name() = form.get("recipient_name");
                    user.sender_name() = form.get("sender_name");
                    user.recipient_addres() = form.get("recipient_addres");
                    user.sender_addres() = form.get("sender_addres");
                    user.date() = form.get("date");
                    user.state() = form.get("state");

                    bool check_result = true;
                    std::string message;
                    std::string reason;

                    if (check_result)
                    {
                        user.save_to_mysql();
                        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                        response.setChunkedTransferEncoding(true);
                        response.setContentType("application/json");
                        std::ostream &ostr = response.send();
                        ostr << user.get_id();
                        return;
                    }
                    else
                    {
                        response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                        std::ostream &ostr = response.send();
                        ostr << message;
                        response.send();
                        return;
                    }
                }
            }
        }
        catch (...)
        {
        }

        response.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND);
        response.setChunkedTransferEncoding(true);
        response.setContentType("application/json");
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
        root->set("type", "/errors/not_found");
        root->set("title", "Internal exception");
        root->set("status", Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND);
        root->set("detail", "request ot found");
        root->set("instance", "/user");
        std::ostream &ostr = response.send();
        Poco::JSON::Stringifier::stringify(root, ostr);
    }

private:
    std::string _format;
};
#endif