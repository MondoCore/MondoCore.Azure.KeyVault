using System;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using MondoCore.Common;
using MondoCore.Azure.TestHelpers;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using static Azure.Core.HttpHeader;

namespace MondoCore.Azure.KeyVault.FunctionalTests
{
    [TestClass]
    public class KeyVaultBlobStoreTests
    {
        [TestMethod]
        public async Task KeyVaultBlobStore_Get()
        {
            var store = CreateStore();

            var id = Guid.NewGuid().ToString();

            await store.Put(id, "Fred");

            Assert.AreEqual("Fred", await store.Get(id));

            await store.Delete(id);
        }

        [TestMethod]
        [ExpectedException(typeof(FileNotFoundException))]
        public async Task KeyVaultBlobStore_Delete()
        {
            var store = CreateStore();
            var key = Guid.NewGuid().ToId();

            await store.Put(key, "blah");

            Assert.AreEqual("blah", await store.Get(key));

            await store.Delete(key);

            Assert.AreEqual(null, await store.Get(key));
        }

        private static readonly string[] names = new [] {"bio", "photo", "resume", "portfolio"};

        [TestMethod]
        public async Task KeyVaultBlobStore_Find()
        {
            var store = CreateStore();

            await DeleteAllEntries();

            var entries = await CreateEntries(store, names);

            var result = (await store.Find("*.*")).ToList();

            Assert.AreEqual(names.Count(), result.Count());

            await DeleteEntries(store, entries);
        }

        private async Task<string[]> CreateEntries(IBlobStore store, string[] names)
        {
            var ids = names.Select((i)=> Guid.NewGuid().ToId());

            foreach(var id in ids)
            {
                await store.Put(id, "blah");
            }

            return ids.ToArray();
        }

        private async Task DeleteEntries(IBlobStore store, string[] ids)
        {
            foreach(var id in ids)
            {
                await store.Delete(id);
            }

            return;
        }

        private async Task DeleteAllEntries()
        {
            var store = CreateStore();

            var result = new List<string>();

            await store.Enumerate("*.*", async (blob)=>
            {
                await store.Delete(blob.Name);
            }, 
            true);
        }

        [TestMethod]
        public async Task KeyVaultBlobStore_Enumerate()
        {
            var store = CreateStore();

            await DeleteAllEntries();

            await CreateEntries(store, names);

            var result = new List<string>();

            await store.Enumerate("*.*", async (blob)=>
            {
                result.Add(blob.Name);

                await Task.CompletedTask;
            }, 
            false);

            Assert.AreEqual(4, result.Count());
        }

        private IBlobStore CreateStore()
        { 
            var config = TestConfiguration.Load();

            return new KeyVaultBlobStore(new Uri(config.KeyVaultUri));
        }
    }
}
