#include "package.h"
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

    void Package::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();
            Statement create_stmt(session);
            create_stmt << "CREATE TABLE IF NOT EXISTS `Package` (`id` INT NOT NULL AUTO_INCREMENT,"
                        << "`name` VARCHAR(256) NOT NULL,"
                        << "`weight` VARCHAR(256) NOT NULL,"
                        << "`price` VARCHAR(256) NOT NULL,"
                        << "`login` VARCHAR(256) NOT NULL,"
                        << "PRIMARY KEY (`id`), KEY `lo` (`login`));",
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

    Poco::JSON::Object::Ptr Package::toJSON() const
    {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("id", _id);
        root->set("name", _name);
        root->set("weight", _weight);
        root->set("price", _price);
        root->set("login", _login);

        return root;
    }

    Package Package::fromJSON(const std::string &str)
    {
        Package package;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        package.id() = object->getValue<long>("id");
        package.name() = object->getValue<std::string>("name");
        package.weight() = object->getValue<std::string>("weight");
        package.price() = object->getValue<std::string>("price");
        package.login() = object->getValue<std::string>("login");

        return package;
    }

    
    std::optional<Package> Package::read_by_id(long id)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            Package a;
            select << "SELECT id, name, weight, price, login FROM Package where id=?",
                into(a._id),
                into(a._name),
                into(a._weight),
                into(a._price),
                into(a._login),
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

    std::vector<Package> Package::search(std::string login)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<Package> result;
            Package a;
            login += "%";
            select << "SELECT id, name, weight, price, login FROM Package where login LIKE ?",
                into(a._id),
                into(a._name),
                into(a._weight),
                into(a._price),
                into(a._login),
                use(login),
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

    void Package::save_to_mysql()
    {

        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);

            insert << "INSERT INTO Package (name,weight,price,login) VALUES(?, ?, ?, ?)",
                use(_name),
                use(_weight),
                use(_price),
                use(_login),

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

    const std::string &Package::get_login() const
    {
        return _login;
    }

    std::string &Package::login()
    {
        return _login;
    }

    long Package::get_id() const
    {
        return _id;
    }

    const std::string &Package::get_name() const
    {
        return _name;
    }

    const std::string &Package::get_weight() const
    {
        return _weight;
    }

    const std::string &Package::get_price() const
    {
        return _price;
    }

    long &Package::id()
    {
        return _id;
    }

    std::string &Package::name()
    {
        return _name;
    }

    std::string &Package::weight()
    {
        return _weight;
    }

    std::string &Package::price()
    {
        return _price;
    }
}