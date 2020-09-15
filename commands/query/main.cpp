#include "command.hpp"

#include <irods/rodsClient.h>
#include <irods/connection_pool.hpp>
#include <irods/filesystem.hpp>
#include <irods/irods_exception.hpp>

#include <boost/program_options.hpp>
#include <boost/dll.hpp>
#include <boost/progress.hpp>

#include <boost/format.hpp>

#include "experimental_plugin_framework.hpp"

#include <iostream>
#include <string>

namespace fs = irods::experimental::filesystem;
namespace po = boost::program_options;
namespace ia = irods::experimental::api;

namespace {
    std::atomic_bool exit_flag{};

    void handle_signal(int sig)
    {
        exit_flag = true;
    }
}



namespace irods::cli
{
    void print_progress(const std::string& p)
    {
        static boost::progress_display prog{100};
        try {
            auto x = std::stoi(p.c_str(), nullptr, 10);
            while(prog.count() != x) {
                ++prog;
            }
        }
        catch(...) {
        }
    } // print_progress

    class rm : public command
    {
    public:
        auto name() const noexcept -> std::string_view override
        {
            return "query";
        }

        auto description() const noexcept -> std::string_view override
        {
            return "Command for queries to the catalog";
        }

        auto help_text() const noexcept -> std::string_view override
        {
            auto help =R"(
Perform a general query against the iRODS catalog

irods query [options] "general query string"

      --progress          : request progress as a percentage)";
            return help;

        }

        auto print_formatted(const std::string& _fmt, const json& _arr) -> void
        {
            boost::format formatter(_fmt);

            try {
                for(auto x : _arr) {
                    formatter % x;
                }
                std::cout << formatter << std::endl;
            }
            catch ( const boost::io::format_error& _e ) {
                std::cout << _e.what() << std::endl;
            }
        } // print_formatted

        auto execute(const std::vector<std::string>& args) -> int override
        {
            signal(SIGINT,  handle_signal);
            signal(SIGHUP,  handle_signal);
            signal(SIGTERM, handle_signal);

            bool progress_flag{false};
            int limit{}, offset{}, page_size{};
            std::string format{};

            using rep_type = fs::object_time_type::duration::rep;

            po::options_description desc{""};
            desc.add_options()
                ("query", po::value<std::string>(), "the query using the general query syntax")
                ("format", po::value<std::string>(&format), "formatting string using fmt syntax")
                ("limit", po::value<int>(&limit), "limit the numbner of query results")
                ("offset", po::value<int>(&offset), "offset of query results")
                ("page_size", po::value<int>(&page_size), "page size for potentially large queries")
                ("progress", po::bool_switch(&progress_flag), "request progress as a percentage");

            po::positional_options_description pod;
            pod.add("query", 1);

            po::variables_map vm;
            po::store(po::command_line_parser(args).options(desc).positional(pod).run(), vm);
            po::notify(vm);

            if (vm.count("query") == 0) {
                std::cerr << "Error: Missing general query.\n";
                return 1;
            }

            rodsEnv env;

            if (getRodsEnv(&env) < 0) {
                std::cerr << "Error: Could not get iRODS environment.\n";
                return 1;
            }

            const auto query = vm["query"].as<std::string>();

            irods::connection_pool conn_pool{1, env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 600};
            auto conn = conn_pool.get_connection();

            std::string progress{};

            auto progress_handler = progress_flag ? print_progress : [](const std::string&) {};

            json req{{"progress", progress_flag},
                     {"query",    query}};

            if(limit     > 0) { req["limit"] = limit; }
            if(offset    > 0) { req["offset"] = offset; }
            if(page_size > 0) { req["page_size"] = page_size; }

            auto cli = ia::client{};
            auto rep = cli(conn,
                           exit_flag,
                           progress_handler,
                           [&](const json& req, const json& rep) -> json {
                               if(rep.contains("results")) {
                                   if(format.empty()) {
                                       std::cout << rep.at("results").dump(4) << "\n";
                                   }
                                   else {
                                       for(auto& arr : rep.at("results")) {
                                           print_formatted(format, arr);
                                       }
                                   }
                               }

                               // request another trip to the endpoint
                               auto tmp = req;
                               tmp.update(rep);
                               return tmp;
                           },
                           req,
                           "query");

            if(exit_flag) {
                std::cout << "Operation Cancelled.\n";
            }

            if(rep.contains("errors")) {
                for(auto e : rep.at("errors")) {
                    std::cout << e << "\n";
                }
            }

            return 0;
        }

    }; // class rm

} // namespace irods::cli

// TODO Need to investigate whether this is truely required.
extern "C" BOOST_SYMBOL_EXPORT irods::cli::rm cli_impl;
irods::cli::rm cli_impl;

