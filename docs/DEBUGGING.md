# Debugging Guide

The MyBibliotheca application includes a powerful and flexible debugging system designed to help you troubleshoot issues without needing to modify the core application code. By enabling specific environment variables, you can get detailed, real-time insights into different components of the application, such as authentication, session management, and CSRF protection.

## How to Enable Debugging

Debugging is controlled through environment variables, which you can set in your `.env` file. To enable a specific debug feature, simply set the corresponding variable to `true`.

Here are the available debug flags:

| Environment Variable        | Description                                                                                                                              |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `MYBIBLIOTHECA_DEBUG`         | This is the master switch for all debugging. If this is set to `false`, no debug messages will be printed, regardless of the other flags. |
| `MYBIBLIOTHECA_DEBUG_AUTH`    | Enables detailed logging for authentication-related events. This includes user login attempts, session loading, and the initial setup process. |
| `MYBIBLIOTHECA_DEBUG_CSRF`    | Provides verbose output for Cross-Site Request Forgery (CSRF) token generation and validation. This is extremely useful for debugging form submission issues. |
| `MYBIBLIOTHECA_DEBUG_SESSION` | Logs detailed information about the user's session, such as when a session is created, modified, or accessed.                               |
| `MYBIBLIOTHECA_DEBUG_REQUESTS`| Prints information about incoming web requests, including the request method, path, and form data.                                         |
| `MYBIBLIOTHECA_VERBOSE_INIT`  | Controls verbose logging during application initialization. When set to `false` (default in production), reduces duplicate startup messages when running with multiple Gunicorn workers. |

### Example `.env` Configuration

To enable all debugging features, you would add the following to your `.env` file:

```env
MYBIBLIOTHECA_DEBUG=true
MYBIBLIOTHECA_DEBUG_AUTH=true
MYBIBLIOTHECA_DEBUG_CSRF=true
MYBIBLIOTHECA_DEBUG_SESSION=true
MYBIBLIOTHECA_DEBUG_REQUESTS=true
MYBIBLIOTHECA_VERBOSE_INIT=true
```

## How It Works

The application's debugging functionality is implemented in the `app/debug_utils.py` module. This module provides a set of specialized logging functions (e.g., `debug_auth`, `debug_csrf`) that are used throughout the application.

When one of these functions is called, it first checks if the master `MYBIBLIOTHECA_DEBUG` flag is enabled. If it is, the function then checks its own specific flag (e.g., `MYBIBLIOTHECA_DEBUG_AUTH`). If both are enabled, a detailed log message is printed to the console.

This approach allows you to selectively enable logging for only the parts of the application you are interested in, keeping your console output clean and focused.

## Common Use Cases

- **Troubleshooting Login Issues:** If users are unable to log in or register, enabling `MYBIBLIOTHECA_DEBUG_AUTH` can provide step-by-step details of the authentication process, helping you pinpoint the cause of the failure.
- **Fixing Form Submission Errors:** If you are encountering "CSRF token missing" or other form-related errors, `MYBIBLIOTHECA_DEBUG_CSRF` and `MYBIBLIOTHECA_DEBUG_SESSION` are invaluable for understanding how the CSRF token and session are being managed.
- **Understanding Application Flow:** `MYBIBLIOTHECA_DEBUG_REQUESTS` can help you trace the flow of a request through the application, which is useful for understanding how different parts of the code interact.

## Reducing Startup Message Duplication

When running MyBibliotheca with multiple Gunicorn workers (default configuration), you may see duplicate initialization messages in the logs. This happens because each worker process initializes independently. To reduce log verbosity in production:

```env
# Disable verbose initialization messages (recommended for production)
MYBIBLIOTHECA_VERBOSE_INIT=false
```

To enable verbose initialization logging (useful for debugging startup issues):

```env
# Enable verbose initialization messages (useful for development/debugging)
MYBIBLIOTHECA_VERBOSE_INIT=true
```

**Note:** Essential error messages (such as Redis connection failures) will always be displayed regardless of this setting.
