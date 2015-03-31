{:dev  {:env {:processor-database-url "//localhost:5432/pdok"
              :processor-database-user "pdok_owner"
              :processor-database-password "pdok_owner"
              :data-database-url "//localhost:5432/pdok"
              :data-database-user "pdok_owner"
              :data-database-password "pdok_owner"
              }}
 :test {:env {:database-user "test-user"}}}