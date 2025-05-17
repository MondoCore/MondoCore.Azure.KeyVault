using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;

using MondoCore.Common;

namespace MondoCore.Azure.KeyVault
{
    public class KeyVaultBlobStore : IBlobStore
    {
        private readonly SecretClient _client;
        private readonly string       _prefix;

        public KeyVaultBlobStore(Uri uri, string tenantId, string clientId, string secret, string? prefix = null) : this(uri, new ClientSecretCredential(tenantId, clientId, secret), prefix)
        {
        }

        public KeyVaultBlobStore(Uri uri, string? prefix = null) : this(uri, new DefaultAzureCredential(), prefix)
        {
        }

        public KeyVaultBlobStore(Uri uri, TokenCredential credential, string? prefix = null)
        {
            _client = new SecretClient(uri, credential);
            _prefix = prefix ?? "";
        }

        #region IBlobStore

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Delete(string id)
        {
            try
            { 
                await _client.StartDeleteSecretAsync(_prefix + id);
            }
            catch(RequestFailedException rex) when (rex.Status == 404)
            {
                // Do nothing
            }
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task<string> Get(string id, Encoding encoding = null)
        {
            try
            { 
                var secret = await _client.GetSecretAsync(_prefix + id);

                return secret?.Value?.Value;
            }
            catch(RequestFailedException rex) when (rex.Status == 404)
            { 
                throw new FileNotFoundException("Secret not found", rex);
            }
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Get(string id, Stream destination)
        {
            var result = await Get(id);

            await destination.WriteAsync(result);
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task<byte[]> GetBytes(string id)
        {
            var result = await Get(id);

            return UTF8Encoding.UTF8.GetBytes(result);
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public Task<Stream> OpenRead(string id)
        {
            throw new NotSupportedException();
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public Task<Stream> OpenWrite(string id)
        {
            throw new NotSupportedException();
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Put(string id, string content, Encoding encoding = null)
        {
            var secret = new KeyVaultSecret(_prefix + id, content);

            try
            { 
                await _client.SetSecretAsync(secret);
            }
            catch(Exception ex)
            {
                throw;
            }

            return;
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Put(string id, Stream content)
        {
            await Put(id, await content.ReadStringAsync());
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Enumerate(string filter, Func<IBlob, Task> fnEach, bool asynchronous = true)
        {
            try
            { 
                var properties = _client.GetPropertiesOfSecretsAsync().AsPages();

                await foreach(var page in properties)
                {                
                    var blobs = page.Values;

                    foreach(var blob in blobs)
                    {
                        var task = fnEach(new KeyvaultBlob(blob, _prefix));
                    }
                }
            }
            catch(RequestFailedException rex) when (rex.Status == 404)
            {
                // Do nothing
            }
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task<IEnumerable<string>> Find(string filter)
        {
            IAsyncEnumerable<Page<SecretProperties>>? properties = null;

            try
            { 
                properties = _client.GetPropertiesOfSecretsAsync().AsPages();
            }
            catch(RequestFailedException rex) when (rex.Status == 404)
            {
                return Enumerable.Empty<string>();
            }
            catch(Exception ex)
            {
                throw;
            }
            
            var result = new List<string>();

            await foreach(var page in properties!)
            {                
                var blobs = page.Values;

                foreach(var blob in blobs)
                {
                    if(!blob.Name.MatchesWildcard(filter))
                        continue;

                    result.Add(blob.Name);
                }
            }

            return result;
        }
        
        /****************************************************************************/
        /// <inheritdoc/>
        public async IAsyncEnumerable<IBlob> AsAsyncEnumerable()
        {
            var properties = _client.GetPropertiesOfSecretsAsync().AsPages();

            await foreach(var page in properties)
            {                
                var blobs = page.Values;

                foreach(var blob in blobs)
                {
                    yield return new KeyvaultBlob(blob, _prefix);
                }
            }
        }

        /****************************************************************************/
        public class KeyvaultBlob(SecretProperties props, string prefix) : IBlob
        { 
            public string                       Name        => props.Name.Substring(prefix.Length);
            public bool                         Deleted     => false;
            public bool                         Enabled     => props.Enabled ?? true;
            public string?                      Version     => props.Version;
            public string                       ContentType => props.ContentType;
            public DateTimeOffset?              Expires     => props.ExpiresOn;
            public IDictionary<string, string>? Metadata    => null;
            public IDictionary<string, string>? Tags        => props.Tags;
        }

        #endregion
    }
}
