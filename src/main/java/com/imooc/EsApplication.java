package com.imooc;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class EsApplication {

	@Autowired
	private TransportClient client;

	public static void main(String[] args) {
		SpringApplication.run(EsApplication.class, args);
	}
	@GetMapping(value = "/")
	public String index(){
		return "index";
	}

	@GetMapping(value = "/get/people/man")
	public ResponseEntity get(@RequestParam(name = "id",defaultValue = "") String id){

		if(id.isEmpty()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		GetResponse response = this.client.prepareGet("people","man",id).get();

		if(!response.isExists()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity(response.getSource(), HttpStatus.OK);
	}

	@PostMapping(value = "/add/people/man")
	public ResponseEntity add(@RequestParam(name = "name") String name,
							  @RequestParam(name = "country") String country,
							  @RequestParam(name = "age") int age,
							  @RequestParam(name = "date")
									  @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
										  Date date){
		try {
			XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name",name)
                    .field("country",country)
                    .field("age",age)
                    .field("date",date.getTime())
                    .endObject();

			IndexResponse response = this.client.prepareIndex("people","man")
					.setSource(contentBuilder)
					.get();
			return new ResponseEntity(response.getId(),HttpStatus.OK);
		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@DeleteMapping(value = "/delete/people/man")
	public ResponseEntity delete(@RequestParam(name = "id") String id){
		DeleteResponse response = this.client.prepareDelete("people","man",id).get();

		return new ResponseEntity(response.getResult(),HttpStatus.OK);
	}

	@PutMapping(value = "/update/people/man")
	public ResponseEntity update(@RequestParam(name = "id") String id,
								 @RequestParam(name = "name") String name){
		UpdateRequest request = new UpdateRequest("people","man",id);

		try {
			XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
					.field("name",name)
					.endObject();
			request.doc(builder);
		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		try {
			UpdateResponse response = this.client.update(request).get();

			return new ResponseEntity(response.getResult(),HttpStatus.OK);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
	}

	@PostMapping(value = "/query/people/man")
	public ResponseEntity query(@RequestParam(name = "gtage") int gtage,
								@RequestParam(name = "ltage") int ltage,
								@RequestParam(name = "name") String name){
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		boolQueryBuilder.must(QueryBuilders.matchQuery("name",name));

		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("age").from(gtage).to(ltage);

		boolQueryBuilder.filter(queryBuilder);

		SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("people")
				.setTypes("man")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(boolQueryBuilder)
				.setFrom(0)
				.setSize(10);
		System.out.println(searchRequestBuilder);

		SearchResponse response = searchRequestBuilder.get();

		List<Map<String,Object>>list = new ArrayList<Map<String,Object>>();

		for (SearchHit documentFields : response.getHits()) {
			list.add(documentFields.getSourceAsMap());
		}
		return new ResponseEntity(list,HttpStatus.OK);
	}
}
