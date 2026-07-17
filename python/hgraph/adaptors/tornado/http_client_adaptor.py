"""Tornado HTTP client implemented as a Python service adaptor."""

import base64
import logging
import os
import re
import socket
import time
from dataclasses import dataclass
from urllib.parse import urlencode, urlparse

from frozendict import frozendict
from tornado.httpclient import AsyncHTTPClient, HTTPError

from hgraph import (
    GlobalState,
    TS,
    TSD,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)

from ._tornado_web import TornadoWeb
from .http_server_adaptor import (
    HttpDeleteRequest,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpRequest,
    HttpResponse,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str

    def __repr__(self) -> str:
        return "credentials"

    __str__ = __repr__


@service_adaptor
def http_client_adaptor(
    request: TS[HttpRequest], path: str = "http_client"
) -> TS[HttpResponse]:
    """Send a GET, POST, PUT, or DELETE request and emit its response."""


def _request_method_and_body(request: HttpRequest) -> tuple[str, str | None]:
    if isinstance(request, HttpGetRequest):
        return "GET", None
    if isinstance(request, HttpPostRequest):
        return "POST", request.body
    if isinstance(request, HttpPutRequest):
        return "PUT", request.body
    if isinstance(request, HttpDeleteRequest):
        return "DELETE", None
    raise TypeError(f"unsupported HTTP request type: {type(request).__name__}")


def _authentication_scheme(header: str) -> str:
    value = header.lower()
    if "negotiate" in value:
        return "Negotiate"
    if "ntlm" in value:
        return "NTLM"
    raise HTTPError(401, "unhandled authentication protocol")


def _canonical_host(url: str) -> str:
    host = urlparse(url).hostname
    if host is None:
        raise HTTPError(401, "authentication request has no host")
    try:
        return socket.getaddrinfo(host, None, 0, 0, 0, socket.AI_CANONNAME)[0][3]
    except socket.gaierror as error:
        logger.info("Skipping canonicalization of %s: %s", host, error)
        return host


def _challenge_token(header: str, scheme: str) -> bytes:
    match = re.search(rf"{re.escape(scheme)}\s*([^,]*)", header, re.IGNORECASE)
    if match is None:
        raise HTTPError(401, f"No {scheme} authentication token found")
    token = match.group(1).strip()
    return base64.b64decode(token) if token else b""


def _copy_response_cookie(response) -> None:
    cookie = response.headers.get("set-cookie")
    if cookie is not None:
        response.request.headers["Cookie"] = cookie


async def _handle_auth(response, request: HttpRequest, client):
    import spnego

    auth_header = response.headers.get("www-authenticate")
    if not auth_header:
        raise HTTPError(401, "missing www-authenticate header")

    scheme = _authentication_scheme(auth_header)
    if scheme == "Negotiate":
        protocol = "kerberos"
        username = password = None
    else:
        protocol = "ntlm"
        if not isinstance(request.auth, Credentials):
            raise HTTPError(
                401,
                "NTLM authentication on non-Windows hosts requires credentials",
            )
        username = request.auth.username
        password = request.auth.password

    context = spnego.client(
        username=username,
        password=password,
        hostname=_canonical_host(response.request.url),
        service="HTTP",
        channel_bindings=None,
        context_req=spnego.ContextReq.sequence_detect
        | spnego.ContextReq.mutual_auth,
        protocol=protocol,
    )

    for _ in range(2):
        challenge = _challenge_token(auth_header, scheme)
        output_token = context.step(in_token=challenge)
        response.request.headers["Authorization"] = (
            f"{scheme} {base64.b64encode(output_token).decode()}"
        )
        _copy_response_cookie(response)
        response = await client.fetch(response.request, raise_error=False)
        auth_header = response.headers.get("www-authenticate", "")
        if response.code == 401:
            continue

        if auth_header:
            try:
                final_token = _challenge_token(auth_header, scheme)
                if final_token:
                    context.step(in_token=final_token)
            except spnego.exceptions.SpnegoError as error:
                raise HTTPError(401, "Kerberos authentication failed") from error
        return response

    raise HTTPError(401, f"{scheme} authentication failed")


async def _handle_auth_win(response, _request: HttpRequest, client):
    import pywintypes
    import sspi
    import sspicon
    import win32security

    auth_header = response.headers.get("www-authenticate")
    if not auth_header:
        raise HTTPError(401, "missing www-authenticate header")
    scheme = _authentication_scheme(auth_header)
    package = win32security.QuerySecurityPackageInfo(scheme)
    authentication = sspi.ClientAuth(
        scheme,
        targetspn=f"HTTP/{_canonical_host(response.request.url)}",
        auth_info=None,
        scflags=sspicon.ISC_REQ_MUTUAL_AUTH,
        datarep=sspicon.SECURITY_NETWORK_DREP,
    )

    security_buffer = win32security.PySecBufferDescType()
    for _ in range(3):
        challenge = _challenge_token(auth_header, scheme)
        if challenge:
            token_buffer = win32security.PySecBufferType(
                package["MaxToken"],
                sspicon.SECBUFFER_TOKEN,
            )
            token_buffer.Buffer = challenge
            security_buffer.append(token_buffer)
        try:
            _, auth = authentication.authorize(security_buffer)
        except pywintypes.error as error:
            raise HTTPError(401, f"Windows authentication failed: {error}") from error

        response.request.headers["Authorization"] = (
            f"{scheme} {base64.b64encode(auth[0].Buffer).decode('ASCII')}"
        )
        _copy_response_cookie(response)
        response = await client.fetch(response.request, raise_error=False)
        auth_header = response.headers.get("www-authenticate", "")
        if response.code != 401:
            final_token = _challenge_token(auth_header, scheme) if auth_header else b""
            if final_token:
                token_buffer = win32security.PySecBufferType(
                    package["MaxToken"],
                    sspicon.SECBUFFER_TOKEN,
                )
                token_buffer.Buffer = final_token
                final_buffer = win32security.PySecBufferDescType()
                final_buffer.append(token_buffer)
                authentication.authorize(final_buffer)
            return response

        security_buffer = win32security.PySecBufferDescType()

    raise HTTPError(401, f"{scheme} authentication failed")


@service_adaptor_impl(interfaces=http_client_adaptor)
def http_client_adaptor_impl(
    request: TSD[int, TS[HttpRequest]],
    path: str = "http_client",
    use_curl: bool = False,
    max_clients: int = 50,
) -> TSD[int, TS[HttpResponse]]:
    """Multiplex graph clients over Tornado's asynchronous HTTP client."""
    if use_curl:
        AsyncHTTPClient.configure(
            "tornado.curl_httpclient.CurlAsyncHTTPClient",
            max_clients=max_clients,
        )

    queue_key = f"http_client_adaptor://{path}/queue"

    @push_queue(TSD[int, TS[HttpResponse]])
    def from_web(sender) -> None:
        GlobalState.instance()[queue_key] = sender

    async def make_http_request(
        request_id: int,
        request_value: HttpRequest,
        sender,
    ) -> None:
        started = time.perf_counter_ns()
        try:
            method, body = _request_method_and_body(request_value)
            query = urlencode(request_value.query)
            url = f"{request_value.url}?{query}" if query else request_value.url
            client = AsyncHTTPClient()
            response = await client.fetch(
                url,
                method=method,
                headers=request_value.headers,
                body=body,
                connect_timeout=request_value.connect_timeout,
                request_timeout=request_value.request_timeout,
                raise_error=False,
            )
            if (
                response.code == 401
                and response.headers.get("www-authenticate") is not None
            ):
                try:
                    if os.name == "nt":
                        response = await _handle_auth_win(
                            response,
                            request_value,
                            client,
                        )
                    else:
                        response = await _handle_auth(
                            response,
                            request_value,
                            client,
                        )
                except HTTPError as error:
                    message = getattr(error, "message", None) or str(error)
                    sender(
                        {
                            request_id: HttpResponse(
                                status_code=error.code,
                                body=message.encode(),
                            )
                        }
                    )
                    return
            result = HttpResponse(
                status_code=response.code,
                headers=frozendict(response.headers.items()),
                body=response.body,
            )
        except Exception as error:
            logger.exception("HTTP request %s failed", request_id)
            result = HttpResponse(status_code=400, body=str(error).encode())

        logger.debug(
            "HTTP request %s completed in %d ms",
            request_id,
            (time.perf_counter_ns() - started) // 1_000_000,
        )
        sender({request_id: result})

    @sink_node
    def to_web(
        requests: TSD[int, TS[HttpRequest]],
        _global_state: GlobalState = None,
    ) -> None:
        sender = _global_state[queue_key]
        loop = TornadoWeb.get_loop()
        for request_id, request_value in requests.modified_items():
            loop.add_callback(
                make_http_request,
                request_id,
                request_value.value,
                sender,
            )

    @to_web.start
    def to_web_start() -> None:
        TornadoWeb.start_loop()

    @to_web.stop
    def to_web_stop(_global_state: GlobalState = None) -> None:
        try:
            TornadoWeb.stop_loop()
        finally:
            if queue_key in _global_state:
                del _global_state[queue_key]

    to_web(request)
    return from_web()


__all__ = (
    "Credentials",
    "http_client_adaptor",
    "http_client_adaptor_impl",
)
