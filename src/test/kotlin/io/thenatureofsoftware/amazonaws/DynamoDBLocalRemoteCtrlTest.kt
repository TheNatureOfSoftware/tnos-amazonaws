package io.thenatureofsoftware.amazonaws


import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.junit.After
import org.junit.Test as test

class DynamoDBLocalRemoteCtrlTest {

    @test
    fun checkDynamoDBLocalHome() {
        val home = System.getenv("DYNAMODB_LOCAL_HOME") ?: ""
        assert.that(DynamoDBLocalRemoteCtrl.dynamoDbLocalHome(), equalTo(home))
    }

    @test
    fun shouldStartDynamoDb() {
        val handle = DynamoDBLocalRemoteCtrl.start(Config())
        assertThatDynamoDBIsRunning(handle)
    }

    @test
    fun shouldStartDynamoDbPersistent() {
        val handle = DynamoDBLocalRemoteCtrl.start(Config(8100, true, DynamoDBLocalRemoteCtrl.dynamoDbLocalHome() + "/db"))
        assertThatDynamoDBIsRunning(handle)
    }

    @test
    fun shouldStartDynamoDbWithDelayTransientStatuses() {
        val handle = DynamoDBLocalRemoteCtrl.start(Config(8100,true, DynamoDBLocalRemoteCtrl.dynamoDbLocalHome() + "/db", true))
        assertThatDynamoDBIsRunning(handle)
    }

    @test
    fun shouldTryToStop() {
        assert.that(DynamoDBLocalRemoteCtrl.stop(), equalTo(false))
    }

    @After
    fun tearDown() {
        DynamoDBLocalRemoteCtrl.stop()
    }

    fun assertThatDynamoDBIsRunning(handle: Handle) {
        handle.amazonDynamoDB.listTables().tableNames.size
        assert.that(handle.amazonDynamoDB.listTables().tableNames.size, equalTo(0))
    }
}