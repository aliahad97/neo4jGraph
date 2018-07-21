package main

import (
	"fmt"
	"time"
	// "io"
	s "strings"
	set "gopkg.in/fatih/set.v0"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver" //neo4j driver
	"net/http" //used for https requests
	// "reflect" // used for the 'TypeOf' function
	// "encoding/json" //not used 
	"github.com/Jeffail/gabs" //used for JSON parsing
	"io/ioutil" //Used for reading the Body of web requests
	// "github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph" //not used
)

const (
	URI	= "bolt://neo4j:admin@localhost:11005"
)

func main() {
	start := time.Now()
	fmt.Println("Emptying database...")
	deleteAll()
	fmt.Println("done.")
	fmt.Println("Setting Up Constraints..")
	run_constraints()
	fmt.Println("done.")
	fmt.Println("Inserting all User Nodes")
	JSON_user:=getUserJSON()
	addAllUsers(JSON_user)
	fmt.Println("done.")
	fmt.Println("Making all User Following relationships")
	addFollows(JSON_user)
	fmt.Println("done.")
	fmt.Println("Linking all Users to the cities (Note: Users without any city field will be ignored)")
	addUserCity(JSON_user)
	JSON_restaurant := getRestaurantJSON()
	fmt.Println("Inserting Restaurant nodes..")
	addAllRestaurants(JSON_restaurant)
	fmt.Println("Linking restaurants to relevent cities")
	addRestaurantCity(JSON_restaurant)
	fmt.Println("done")
	fmt.Println("Inserting All user following restaurant relationships")
	addUserFollowRestaurant(JSON_restaurant)
	fmt.Println("done")
	fmt.Println("Inserting Cusines")
	addCusine(JSON_restaurant)
	fmt.Println("done")
	fmt.Println("Inserting Categories")
	addCategory(JSON_restaurant)
	fmt.Println("done")
	JSON_review := getReviewJSON()
	fmt.Println("Inserting Reviews")
	addAllReviews(JSON_review)
	fmt.Println("All Reviews have been added")
	fmt.Println("Inserting reviewers link to reviews...")
	addReviewer(JSON_review)
	fmt.Println("done..")
	fmt.Println("Creating review links to restaurants..")
	addRestaurantReview(JSON_review)
	fmt.Println("done")
	fmt.Println("Inserting Categories to reviews")
	addReviewCategory(JSON_review)
	fmt.Println("done")
	fmt.Println("Inserting link of Users that LIKE a review")
	addReviewLike(JSON_review)
	fmt.Println("done")

	elapsed := time.Since(start)
    fmt.Printf("Everything completed in %s", elapsed)

}
func addReviewLike(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Review count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		key:=jsonParsed.Index(i).S("_key").String()
		dishes,_:=jsonParsed.Index(i).S("likedBy").Children()
		for _,dish := range dishes {
			createReviewLikeRelationship(key,dish.Data().(string))
		}
	}
}
func addReviewCategory(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Review count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		key:=jsonParsed.Index(i).S("_key").String()
		dishes,_:=jsonParsed.Index(i).S("dishes").Children()
		for _,dish := range dishes {
			cat:=dish.S("category").String()
			createReviewCategoryRelationship(key,cat)
		}
	}
}
func addRestaurantReview(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Review count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		key:= jsonParsed.Index(i).S("_key").String()
		restaurant_obj:=jsonParsed.Index(i).S("selectedRestaurant")
		ids:=restaurant_obj.S("_id").String()
		if ids!="{}" {
			createRestaurantReviewRelationship(key,ids)
		}
	}
}
func addUserCity(jsonParsed *gabs.Container){
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("User count: ",count)
	
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		ids:=jsonParsed.Index(i).S("_id").String()
		city:=jsonParsed.Index(i).S("city").String()
		//run the following if condition if and only if there is a city
		if city!="{}" {
			createUserCityRelationship(ids,city)
		}
	}
}
func addRestaurantCity(jsonParsed *gabs.Container){
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Restaurant count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		ids:=jsonParsed.Index(i).S("_id").String()
		city:=jsonParsed.Index(i).S("city").String()
		//run the following if condition if and only if there is a city
		if city!="{}" {
			createRestaurantCityRelationship(ids,city)
		}
	}
}
func addCategory(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Restaurant count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		ids:=jsonParsed.Index(i).S("_id").String()
		ids = s.Replace(ids,"\"","",-1)
		menuParsed:=getRestaurantMenuJSON(ids)
		category := menuParsed.S("menu").String()
		if category!="{}" {
			obj,_:=menuParsed.S("menu").Children()
			s:=set.New() //set to enter unique categories

			for _,i :=range obj  {
				obj1:= i.S("category").String()
				s.Add(obj1)
			}
			list_of_categories := s.List()
			
			for _,cat:=range list_of_categories{
				createRestaurantCategoryRelationship(ids,cat.(string))
			}

			
		}
	}
}
func addCusine(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Restaurant count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		ids:=jsonParsed.Index(i).S("_id").String()
		cusine:=jsonParsed.Index(i).S("cusine").String()
		//run the following if condition if and only if there is a cusine
		//Most restaurants are put in 'TBA' cusine 
		if cusine!="{}" {
			split:= s.Split(cusine,", ")
			for _,cus:= range split{
				cus1:=s.Replace(cus,"\"","",-1)
				if s.HasSuffix(cus1," ") {
					cus1 = cus1[:len(cus1)-1]
				}
				createRestaurantCusineRelationship(ids,cus1)
			}
		}
	}
}
func addUserFollowRestaurant(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Restaurant count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		id:=jsonParsed.Index(i).S("_id").String()
		follows,_:=jsonParsed.Index(i).S("followedBy").Children()
		for _,user := range follows {
			createUserRestaurantRelationship(id,user.Data().(string))
		}
	}
}
func addFollows(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("User count: ",count)
	
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		ids:=jsonParsed.Index(i).S("_id").String()
		check_follower:=jsonParsed.Index(i).S("follows").String()
		//run the following if condition if and only if there is a follower
		if check_follower!="{}" {
			obj := jsonParsed.Index(i).S("follows")
			chil,_ := obj.S("id").Children()
			// handleError(err)
			for _, child := range chil {
				createFollowRelationship(child.Data().(string) , ids )
			}
		}
	}
}
func addReviewer(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Review count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		
		key:= jsonParsed.Index(i).S("_key").String()
		user_obj:=jsonParsed.Index(i).S("user")
		ids:=user_obj.S("_id").String()
		if ids!="{}" {
			createUserReviewRelationship(key,ids)
		}
	}
}
func addAllUsers(jsonParsed *gabs.Container) {
	// fmt.Println(reflect.TypeOf(jsonParsed))
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("User count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		naam:=jsonParsed.Index(i).S("firstName").String()
		ids:=jsonParsed.Index(i).S("_id").String()
		// fmt.Println(naam,ids)
		createUserNode(ids,naam)
	}
}
func addAllReviews(jsonParsed *gabs.Container) {
	// fmt.Println(reflect.TypeOf(jsonParsed))
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Review count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		key:=jsonParsed.Index(i).S("_key").String()
		// fmt.Println(naam,ids)
		createReviewNode(key)
	}
}
func addAllRestaurants(jsonParsed *gabs.Container) {
	count,_ :=jsonParsed.ArrayCount()
	fmt.Println("Restaurant count: ",count)
	//iterate through all and add them into the database
	for i := 0; i < count; i++ {
		naam:=jsonParsed.Index(i).S("name").String()
		ids:=jsonParsed.Index(i).S("_id").String()
		// fmt.Println(naam,ids,i)
		createRestaurantNode(ids,naam)
	}
}
func deleteAll() {
	conn:= createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="MATCH (n) DETACH DELETE n"
	stmt:= prepareSatement(str,conn)
	executeStatement(stmt)
}
func createUserRestaurantRelationship(id string, user string) {
	// fmt.Println(id, user)
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (r: Restaurant{id:"+id+"}) Match (n:User{id:'"+user+"'}) CREATE (n)-[:FOLLOWS]->(r)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createReviewLikeRelationship(key string, id string) {
	// fmt.Println(key,id)
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (r: Review{key:"+key+"}) Match (n:User{id:'"+id+"'}) CREATE (n)-[:LIKES]->(r)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createReviewCategoryRelationship(key string,cat string){
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (r: Review{key:"+key+"}) MERGE (n:Category{name:"+cat+"}) CREATE (r)-[:REVIEW_CATEGORY]->(n)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createRestaurantReviewRelationship(key string,id string) {
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (n:Restaurant{id:"+id+"}) Match (r: Review{key:"+key+"}) CREATE (r)-[:REVIEW_RESTAURANT]->(n)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createRestaurantCategoryRelationship(id string, category string) {
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (n:Restaurant{id:'"+id+"'}) MERGE (b: Category{name:"+category+"}) CREATE (n)-[:SERVES]->(b)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createUserReviewRelationship(key string, id string) {
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (n:User{id:"+id+"}) Match (r: Review{key:"+key+"}) CREATE (n)-[:REVIEWED]->(r)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)

}
func createUserCityRelationship(id string, city string) {
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (n:User{id:"+id+"}) MERGE (b: City{name:"+city+"}) CREATE (n)-[:IN]->(b)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createRestaurantCusineRelationship(id string, cusine string) {
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (n:Restaurant{id:"+id+"}) MERGE (b: Cusine{name:'"+cusine+"'}) CREATE (n)-[:HAS]->(b)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createRestaurantCityRelationship(id string, city string) {
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (n:Restaurant{id:"+id+"}) MERGE (b: City{name:"+city+"}) CREATE (n)-[:IN]->(b)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createFollowRelationship(follows string, id string) {
	conn:=createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="Match (n:User{id:"+id+"}) Match (b: User{id:'"+follows+"'}) CREATE (n)-[:FOLLOWS]->(b)"
	// fmt.Println(str)
	stmt :=prepareSatement(str,conn)
	executeStatement(stmt)
}
func createReviewNode(key string) {
	conn:= createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="CREATE (n:Review {key :"+key+"})"
	stmt:= prepareSatement(str,conn)
	executeStatement(stmt)
}
func createRestaurantNode(id string, name string) {
	conn:= createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="CREATE (n:Restaurant {id :"+id+",name:"+name+"})"
	stmt:= prepareSatement(str,conn)
	executeStatement(stmt)
}

func createUserNode(id string, name string) {
	conn:= createConnection()
	defer conn.Close() // push this on the stack to close connection as soon as something goes wrong
	//prepare stmt
	str :="CREATE (n:User {id :"+id+",name:"+name+"})"
	stmt:= prepareSatement(str,conn)
	executeStatement(stmt)
}

func getUserJSON() *gabs.Container {
	resp, err := http.Get("https://api.paitoo.com.pk/users/all")
	handleError(err)
	body,err := ioutil.ReadAll(resp.Body)
	handleError(err)
	jsonParsed, err := gabs.ParseJSON(body)
	handleError(err)
	return jsonParsed
}
func getReviewJSON() *gabs.Container {
	resp, err := http.Get("https://api.paitoo.com.pk/reviews/all")
	handleError(err)
	body,err := ioutil.ReadAll(resp.Body)
	handleError(err)
	jsonParsed, err := gabs.ParseJSON(body)
	handleError(err)
	return jsonParsed
}
func getRestaurantJSON() *gabs.Container {
	resp, err := http.Get("https://api.paitoo.com.pk/restaurants/all")
	handleError(err)
	body,err := ioutil.ReadAll(resp.Body)
	handleError(err)
	jsonParsed, err := gabs.ParseJSON(body)
	handleError(err)
	return jsonParsed
}

func getRestaurantMenuJSON(id string) *gabs.Container{
	URL := "https://api.paitoo.com.pk/restaurants/restaurant/"+id
	resp, err := http.Get(URL)
	handleError(err)
	body,err := ioutil.ReadAll(resp.Body)
	handleError(err)
	jsonParsed, err := gabs.ParseJSON(body)
	handleError(err)
	return jsonParsed

}

func createConnection() bolt.Conn { 
	driver := bolt.NewDriver()
	con, err := driver.OpenNeo(URI)
	handleError(err)
	return con
}

// Here we prepare a new statement. This gives us the flexibility to
// cancel that statement without any request sent to Neo
func prepareSatement(query string, con bolt.Conn) bolt.Stmt {
	st, err := con.PrepareNeo(query)
	handleError(err)
	return st
}


// Executing a statement just returns summary information
func executeStatement(st bolt.Stmt) {
	_, err := st.ExecNeo(nil)
	handleError(err)
	// _, err = result.RowsAffected()
	// handleError(err)
	// fmt.Printf("CREATED ROWS: %d\n", numResult)

	// Closing the statment will also close the rows
	st.Close()
}

func consumeRows(rows bolt.Rows, st bolt.Stmt) {
	// This interface allows you to consume rows one-by-one, as they
	// come off the bolt stream. This is more efficient especially
	// if you're only looking for a particular row/set of rows, as
	// you don't need to load up the entire dataset into memory
	_, _, err := rows.NextNeo()
	for err == nil {
		_, _, err = rows.NextNeo()
	}
	handleError(err)
	// This query only returns 1 row, so once it's done, it will return
	// the metadata associated with the query completion, along with
	// io.EOF as the error

	st.Close()
}

func run_constraints(){
	constraintRestaurant:="CREATE CONSTRAINT ON (n:Restaurant) ASSERT n.id IS UNIQUE";
	constraintUser:="CREATE CONSTRAINT ON (m:User) ASSERT m.id IS UNIQUE";
	constraintCusine:="CREATE CONSTRAINT ON (p:Cusine) ASSERT p.name IS UNIQUE";
	constraintCategory:="CREATE CONSTRAINT ON (q:Category) ASSERT q.name IS UNIQUE";
	constraintCity:="CREATE CONSTRAINT ON (c:City) ASSERT c.name IS UNIQUE"
	constraintReview:="CREATE CONSTRAINT ON (r:Review) ASSERT r.key IS UNIQUE"
	
	con := createConnection() //create connection 
	defer con.Close()
	pipeline, err := con.PreparePipeline(
		constraintRestaurant,
		constraintCategory,
		constraintCusine,
		constraintUser,
		constraintCity,
		constraintReview,
	)
	handleError(err)

	_, err = pipeline.ExecPipeline(nil, nil, nil, nil,nil,nil)
	handleError(err)
	err = pipeline.Close()
	handleError(err)
	fmt.Println("Constraints have been set")
}

//Following functionis for error handling
func handleError(err error) {
	if err != nil {
		panic(err)
	}
}