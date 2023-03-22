#ifndef AUTHOR_H
#define AUTHOR_H

#include <string>
#include <vector>
#include "Poco/JSON/Object.h"
#include <optional>

namespace database
{
    class User{
        private:
            long _id;
            std::string _login;
            std::string _password;
            std::string _name;
            std::string _surname;
            std::string _address;

        public:

            static User fromJSON(const std::string & str);

            long             get_id() const;
            const std::string &get_login() const;
            const std::string &get_password() const;
            const std::string &get_name() const;
            const std::string &get_surname() const;
            const std::string &get_address() const;

            long&        id();
            std::string &login();
            std::string &password();
            std::string &name();
            std::string &surname();
            std::string &address();

            static void init();
            static std::optional<User> read_by_id(long id);
            static std::optional<long> auth(std::string &login, std::string &password);
            static std::vector<User> read_all();
            static std::vector<User> search(std::string name,std::string surname);
            void save_to_mysql();

            Poco::JSON::Object::Ptr toJSON() const;

    };
}

#endif