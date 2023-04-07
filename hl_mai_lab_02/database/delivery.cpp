#include "delivery.h"
#include "database.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <sstream>
#include <exception>

using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{

    void Delivery::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();
            Statement create_stmt(session);
            create_stmt << "CREATE TABLE IF NOT EXISTS `Delivery` (`id` INT NOT NULL AUTO_INCREMENT,"
                        << "`recipient_name` VARCHAR(256) NOT NULL,"
                        << "`sender_name` VARCHAR(256) NOT NULL,"
                        << "`recipient_addres` VARCHAR(256) NOT NULL,"
                        << "`sender_addres` VARCHAR(256) NOT NULL,"
                        << "`date` VARCHAR(256) NOT NULL,"
                        << "`state` VARCHAR(256) NULL,"
                        << "PRIMARY KEY (`id`));",
                now;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    Poco::JSON::Object::Ptr Delivery::toJSON() const
    {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("id", _id);
        root->set("recipient_name", _recipient_name);
        root->set("sender_name", _sender_name);
        root->set("recipient_addres", _recipient_addres);
        root->set("sender_addres", _sender_addres);
        root->set("date", _date);
        root->set("state", _state);

        return root;
    }

    Delivery Delivery::fromJSON(const std::string &str)
    {
        Delivery user;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        user.id() = object->getValue<long>("id");
        user.recipient_name() = object->getValue<std::string>("recipient_name");
        user.sender_name() = object->getValue<std::string>("sender_name");
        user.recipient_addres() = object->getValue<std::string>("recipient_addres");
        user.sender_addres() = object->getValue<std::string>("sender_addres");
        user.date() = object->getValue<std::string>("date");
        user.state() = object->getValue<std::string>("state");

        return user;
    }

    
    std::optional<Delivery> Delivery::read_by_id(long id)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            Delivery a;
            select << "SELECT id, state FROM Delivery where id=?",
                into(a._id),
                into(a._state),
                use(id),
                range(0, 1); //  iterate over result set one row at a time

            select.execute();
            Poco::Data::RecordSet rs(select);
            if (rs.moveFirst()) return a;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            
        }
        return {};
    }

    std::optional<Delivery> Delivery::set_state(long id, std::string state)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            Delivery a;
            select << "UPDATE Delivery SET state=? where id=?",
                use(state),
                use(id),
                range(0, 1); //  iterate over result set one row at a time
            select.execute();
            
            Poco::Data::Session session2 = database::Database::get().create_session();
            Poco::Data::Statement select2(session2);
            select2 << "SELECT state FROM Delivery where id=?",
                into(a._state),
                use(id),
                range(0, 1);
            a._id = id;
            select2.execute();
            Poco::Data::RecordSet rs(select2);
            if (a._state==state) return a;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            
        }
        return {};
    }

    std::vector<Delivery> Delivery::read_by_names(std::string recipient_name,std::string sender_name)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            std::vector<Delivery> result;
            Delivery a;
            select << "SELECT id, recipient_name, sender_name, recipient_addres, sender_addres, date, state FROM Delivery where recipient_name=? and sender_name=?",
                into(a._id),
                into(a._recipient_name),
                into(a._sender_name),
                into(a._recipient_addres),
                into(a._sender_addres),
                into(a._date),
                into(a._state),
                use(recipient_name),
                use(sender_name),
                range(0, 1); //  iterate over result set one row at a time
            while (!select.done())
            {
                if (select.execute())
                    result.push_back(a);
            }
            return result;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    void Delivery::save_to_mysql()
    {

        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);

            insert << "INSERT INTO Delivery (recipient_name,sender_name,recipient_addres,sender_addres,date,state) VALUES(?, ?, ?, ?, ?, ?)",
                use(_recipient_name),
                use(_sender_name),
                use(_recipient_addres),
                use(_sender_addres),
                use(_date),
                use(_state);
            insert.execute();

            Poco::Data::Statement select(session);
            select << "SELECT LAST_INSERT_ID()",
                into(_id),
                range(0, 1); //  iterate over result set one row at a time

            if (!select.done())
            {
                select.execute();
            }
            std::cout << "inserted:" << _id << std::endl;
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    const std::string &Delivery::get_recipient_name() const
    {
        return _recipient_name;
    }

    const std::string &Delivery::get_sender_name() const
    {
        return _sender_name;
    }

    std::string &Delivery::recipient_name()
    {
        return _recipient_name;
    }

    std::string &Delivery::sender_name()
    {
        return _sender_name;
    }

    long Delivery::get_id() const
    {
        return _id;
    }

    const std::string &Delivery::get_recipient_addres() const
    {
        return _recipient_addres;
    }

    const std::string &Delivery::get_sender_addres() const
    {
        return _sender_addres;
    }

    const std::string &Delivery::get_date() const
    {
        return _date;
    }
        const std::string &Delivery::get_state() const
    {
        return _state;
    }

    long &Delivery::id()
    {
        return _id;
    }

    std::string &Delivery::recipient_addres()
    {
        return _recipient_addres;
    }

    std::string &Delivery::sender_addres()
    {
        return _sender_addres;
    }

    std::string &Delivery::date()
    {
        return _date;
    }
    std::string &Delivery::state()
    {
        return _state;
    }
}