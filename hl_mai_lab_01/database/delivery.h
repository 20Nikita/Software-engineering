#ifndef AUTHOR_H
#define AUTHOR_H

#include <string>
#include <vector>
#include "Poco/JSON/Object.h"
#include <optional>

namespace database
{
    class Delivery{
        private:
            long _id;
            std::string _recipient_name;
            std::string _sender_name;
            std::string _recipient_addres;
            std::string _sender_addres;
            std::string _date;
            std::string _state;

        public:

            static Delivery fromJSON(const std::string & str);

            long             get_id() const;
            const std::string &get_recipient_name() const;
            const std::string &get_sender_name() const;
            const std::string &get_recipient_addres() const;
            const std::string &get_sender_addres() const;
            const std::string &get_date() const;
            const std::string &get_state() const;

            long&        id();
            std::string &recipient_name();
            std::string &sender_name();
            std::string &recipient_addres();
            std::string &sender_addres();
            std::string &date();
            std::string &state();

            static void init();
            static std::optional<Delivery> read_by_id(long id);
            static std::vector<Delivery> read_by_names(std::string recipient_name,std::string sender_name);
            static std::optional<Delivery> set_state(long id, std::string state);
            void save_to_mysql();

            Poco::JSON::Object::Ptr toJSON() const;

    };
}

#endif