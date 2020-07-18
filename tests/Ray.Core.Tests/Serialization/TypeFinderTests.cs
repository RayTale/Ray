using System;
using FakeItEasy;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Ray.Core.Event;
using Ray.Core.Exceptions;

namespace Ray.Core.Serialization.Tests.Serialization
{
    [TestFixture]
    public class TypeFinderTests
    {
        private TypeFinder sut;
        private ILogger<TypeFinder> logger;

        [SetUp]
        public void Setup()
        {
            this.logger = A.Fake<ILogger<TypeFinder>>();
            this.sut = new TypeFinder(this.logger);
        }

        [Test]
        [TestCase(typeof(TestEventWithDefaultConstructor))]
        [TestCase(typeof(TestEventWithDefaultConstructorAndInterface))]
        [TestCase(typeof(TestEventWithTypeNameSpecifiedInConstructor))]
        [TestCase(typeof(TestEventWithTypeNameSpecifiedInConstructorAndInterface))]
        [TestCase(typeof(TestEventWithNoAttributeAndInterface))]
        public void Can_Find_Test_With_EventNameAttribute(Type type)
        {
            var result = this.sut.FindType(type.FullName);
            result.Should().Be(type);
        }

        [Test]
        public void Invalid_Type_Code_Should_Throw_UnknownTypeCodeException()
        {
            const string fakeType = "not a real type code";
            Action act = () => this.sut.FindType(fakeType);

            act.Should().Throw<UnknownTypeCodeException>()
                .WithMessage(fakeType);
        }

        [Test]
        public void A_ValidEventTypeCode_Should_Be_Found_by_Type()
        {
            var result = this.sut.GetCode(typeof(TestEventWithDefaultConstructor));
            result.Should().Be(typeof(TestEventWithDefaultConstructor).FullName);
        }

        [Test]
        public void An_UnregisteredType_Should_Return_FullName_OfPassedInType()
        {
            var result = this.sut.GetCode(typeof(string));
            result.Should().Be(typeof(string).FullName);
        }

        [EventName]
        private class TestEventWithDefaultConstructor
        {
            public string SomeProperty { get; set; }
        }

        [EventName(nameof(TestEventWithDefaultConstructor))]
        private class TestEventWithDefaultConstructor2
        {
            public string SomeProperty { get; set; }
        }

        [EventName]
        private class TestEventWithDefaultConstructorAndInterface : IEvent
        {
            public string SomeProperty { get; set; }
        }

        [EventName(nameof(TestEventWithTypeNameSpecifiedInConstructor))]
        private class TestEventWithTypeNameSpecifiedInConstructor
        {
            public string SomeProperty { get; set; }
        }

        [EventName(nameof(TestEventWithTypeNameSpecifiedInConstructorAndInterface))]
        private class TestEventWithTypeNameSpecifiedInConstructorAndInterface : IEvent
        {
            public string SomeProperty { get; set; }
        }

        private class TestEventWithNoAttributeAndInterface : IEvent
        {
            public string SomeProperty { get; set; }
        }
    }
}