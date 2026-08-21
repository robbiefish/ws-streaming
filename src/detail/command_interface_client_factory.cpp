#include <cstdint>
#include <memory>

#include <nlohmann/json.hpp>

#include <ws-streaming/detail/command_interface_client.hpp>
#include <ws-streaming/detail/command_interface_client_factory.hpp>
#include <ws-streaming/detail/http_command_interface_client.hpp>
#include <ws-streaming/detail/in_band_command_interface_client.hpp>
#include <ws-streaming/detail/base_peer.hpp>

std::unique_ptr<wss::detail::command_interface_client>
wss::detail::command_interface_client_factory::create_client(
    const nlohmann::json& interfaces,
    const std::shared_ptr<base_peer>& peer)
{
    if (!interfaces.is_object())
        return nullptr;

    // Prefer the in-band command interface ("jsonrpc") if it's supported.
    if (interfaces.contains("jsonrpc"))
        return std::make_unique<in_band_command_interface_client>(peer);

    // Some peers advertise the HTTP command interface fields directly on the
    // commandInterfaces object rather than nested under "jsonrpc-http".
    const nlohmann::json& http =
        interfaces.contains("jsonrpc-http") && interfaces["jsonrpc-http"].is_object()
            ? interfaces["jsonrpc-http"]
            : interfaces;

    if (http.contains("httpMethod")
        && http["httpMethod"].is_string()
        && http.contains("httpPath")
        && http["httpPath"].is_string()
        && http.contains("httpVersion")
        && http["httpVersion"].is_string()
        && http.contains("port")
        && (http["port"].is_string() || http["port"].is_number_integer()))
    {
        std::string port;
        if (http["port"].is_number_integer())
            port = std::to_string(http["port"].is_number_integer());
        else
            port = http["port"];

        std::string remote_endpoint_address;
        try
        {
            auto remote_endpoint = peer->socket().remote_endpoint();
            remote_endpoint_address = remote_endpoint.address().to_string();
        }
        catch (const std::exception& /*e*/)
        {
            return nullptr;
        }

        return std::make_unique<http_command_interface_client>(
            peer->socket().get_executor(),
            remote_endpoint_address,
            port,
            http["httpMethod"],
            http["httpPath"],
            http["httpVersion"]);
    }

    // There are no available/supported command interfaces.
    return nullptr;
}
