using System.Net;

namespace CosmosdUtls
{
    public static class HttpStatusHelper
    {
        public static bool IsSuccessStatusCode(this HttpStatusCode StatusCode)
        {
            int statusCodeInt = (int)StatusCode;
            return statusCodeInt >= 200 && statusCodeInt <= 299;
        }
    }
}
